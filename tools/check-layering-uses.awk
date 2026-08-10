# Tokenize the Rust source just deeply enough to reason about `use` trees and
# crate-root paths. Comments and literals are discarded so examples and doc
# text cannot trip the enforcement check.

function add_token(value, number) {
  token_count++
  tokens[token_count] = value
  token_lines[token_count] = number
}

function is_identifier(value) {
  return value ~ /^[A-Za-z_][A-Za-z0-9_]*$/
}

function repeat(value, count, result) {
  result = ""
  while (count > 0) {
    result = result value
    count--
  }
  return result
}

function begin_raw_string(source, offset, cursor, hashes, character) {
  cursor = offset
  if (substr(source, cursor, 2) == "br") {
    cursor += 2
  } else if (substr(source, cursor, 1) == "r") {
    cursor++
  } else {
    return 0
  }

  hashes = 0
  while (substr(source, cursor, 1) == "#") {
    hashes++
    cursor++
  }
  character = substr(source, cursor, 1)
  if (character != "\"") {
    return 0
  }

  raw_string_end = "\"" repeat("#", hashes)
  return cursor - offset + 1
}

function tokenize(source, number, offset, length_, character, pair, rest, found, raw_length) {
  source = source "\n"
  length_ = length(source)
  offset = 1

  while (offset <= length_) {
    rest = substr(source, offset)

    if (raw_string_end != "") {
      found = index(rest, raw_string_end)
      if (found == 0) {
        return
      }
      offset += found - 1 + length(raw_string_end)
      raw_string_end = ""
      continue
    }

    if (in_string) {
      character = substr(source, offset, 1)
      if (string_escape) {
        string_escape = 0
      } else if (character == "\\") {
        string_escape = 1
      } else if (character == "\"") {
        in_string = 0
      }
      offset++
      continue
    }

    if (block_comment_depth > 0) {
      pair = substr(source, offset, 2)
      if (pair == "/*") {
        block_comment_depth++
        offset += 2
      } else if (pair == "*/") {
        block_comment_depth--
        offset += 2
      } else {
        offset++
      }
      continue
    }

    pair = substr(source, offset, 2)
    if (pair == "//") {
      return
    }
    if (pair == "/*") {
      block_comment_depth = 1
      offset += 2
      continue
    }

    raw_length = begin_raw_string(source, offset)
    if (raw_length > 0) {
      offset += raw_length
      continue
    }

    # Raw identifiers have the same semantic name as the identifier after
    # `r#`. Normalize them to one token so paths and `mod` declarations cannot
    # bypass name or depth checks.
    if (match(rest, /^r#[A-Za-z_][A-Za-z0-9_]*/)) {
      add_token(substr(rest, 3, RLENGTH - 2), number)
      offset += RLENGTH
      continue
    }

    character = substr(source, offset, 1)
    if (character == "\"") {
      in_string = 1
      string_escape = 0
      offset++
      continue
    }

    # Skip a character literal, but leave lifetimes such as `'a` tokenizable.
    if (character == "'") {
      if (match(rest, /^'([^'\\]|\\.)'/)) {
        offset += RLENGTH
        continue
      }
      offset++
      continue
    }

    if (character ~ /[A-Za-z_]/) {
      match(rest, /^[A-Za-z_][A-Za-z0-9_]*/)
      add_token(substr(rest, 1, RLENGTH), number)
      offset += RLENGTH
      continue
    }

    if (pair == "::") {
      add_token("::", number)
      offset += 2
      continue
    }

    if (character ~ /[{}(),;*\[\]]/) {
      add_token(character, number)
    }
    offset++
  }
}

function build_depths(index_, module_depth, stack_depth, opens_module) {
  module_depth = base_module_depth + 0
  stack_depth = 0

  for (index_ = 1; index_ <= token_count; index_++) {
    module_depth_at[index_] = module_depth
    brace_depth_at[index_] = stack_depth

    if (tokens[index_] == "{") {
      opens_module = index_ >= 3 && tokens[index_ - 2] == "mod" \
        && is_identifier(tokens[index_ - 1])
      stack_depth++
      module_braces[stack_depth] = opens_module
      if (opens_module) {
        module_depth++
      }
    } else if (tokens[index_] == "}") {
      if (stack_depth <= 0) {
        parse_error("unmatched closing brace", index_)
        continue
      }
      if (module_braces[stack_depth]) {
        module_depth--
      }
      delete module_braces[stack_depth]
      stack_depth--
    }
  }

  if (stack_depth != 0) {
    parse_error("unterminated brace-delimited source", token_count)
  }
}

function parse_error(message, index_) {
  if (!failed) {
    if (index_ > 0) {
      print FILENAME ":" token_lines[index_] ": " message > "/dev/stderr"
    } else {
      print FILENAME ": " message > "/dev/stderr"
    }
  }
  failed = 1
}

function append_path(prefix, segment) {
  return prefix == "" ? segment : prefix "/" segment
}

function normalized_path(path) {
  sub(/^(crate|self)\//, "", path)
  return path
}

function last_path_segment(path, count, parts) {
  count = split(path, parts, "/")
  return parts[count]
}

function record_import_binding(path, alias, index_, normalized, binding, parent, key) {
  # In lower mode the parsed use-tree items are inspected for crate-root
  # violations instead of contributing to the lib.rs export derivation.
  if (mode == "lower") {
    inspect_lower_use_item(path, 0, index_)
    return
  }

  normalized = normalized_path(path)
  if (alias == "_") {
    return
  }
  if (alias != "") {
    binding = alias
  } else if (last_path_segment(normalized) == "self") {
    parent = normalized
    sub(/\/self$/, "", parent)
    binding = last_path_segment(parent)
  } else {
    binding = last_path_segment(normalized)
  }

  if (!is_identifier(binding)) {
    parse_error("unsupported import binding: " binding, index_)
    return
  }

  key = binding SUBSEP normalized
  if (!import_bindings_seen[key]) {
    import_bindings_seen[key] = 1
    import_binding_count++
    import_bindings[import_binding_count] = binding
    import_paths[import_binding_count] = normalized
  }
}

function record_import_glob(path, index_, normalized, key) {
  if (mode == "lower") {
    inspect_lower_use_item(path, 1, index_)
    return
  }

  normalized = normalized_path(path)
  key = normalized SUBSEP index_
  if (!import_globs_seen[key]) {
    import_globs_seen[key] = 1
    import_glob_count++
    import_glob_paths[import_glob_count] = normalized
    import_glob_indices[import_glob_count] = index_
  }
}

function path_is_crate_root(path, count, parts) {
  count = split(path, parts, "/")
  return count == 1 \
    && (parts[1] == "crate" || parts[1] == "self" || (parts[1] in root_bindings))
}

function path_is_tree_derived(path, count, parts, first) {
  count = split(path, parts, "/")
  first = parts[1]
  # A crate-root alias re-roots the path one segment later: after
  # `use crate as root`, the path `root::tree::X` means `crate::tree::X`.
  if (first in root_bindings) {
    first = parts[2]
  }
  return first == "tree" || (first in tree_bindings)
}

function resolve_tree_bindings(index_, changed, binding) {
  # Rust item order is irrelevant, so resolve aliases to a fixed point rather
  # than depending on the order of the crate-root use statements. This covers
  # chains such as `tree::System as First` followed by `First as Second`, and
  # crate-root aliases such as `use crate as root` followed by
  # `root::tree::System as Third`.
  do {
    changed = 0
    for (index_ = 1; index_ <= import_binding_count; index_++) {
      binding = import_bindings[index_]
      if (!(binding in root_bindings) \
          && path_is_crate_root(import_paths[index_])) {
        root_bindings[binding] = 1
        changed = 1
      }
      if (!(binding in tree_bindings) \
          && path_is_tree_derived(import_paths[index_])) {
        tree_bindings[binding] = 1
        changed = 1
      }
    }
  } while (changed)

  for (index_ = 1; index_ <= import_glob_count; index_++) {
    # A glob of the crate root re-imports every root name, tree exports
    # included, so the derivation cannot stay complete; fail closed.
    if (path_is_crate_root(import_glob_paths[index_])) {
      parse_error("crate-root glob imports cannot be derived safely", \
        import_glob_indices[index_])
      return
    }
    # A glob through a derived alias is just as opaque as `use tree::*`; fail
    # closed once alias resolution has identified its tree-layer source.
    if (path_is_tree_derived(import_glob_paths[index_])) {
      parse_error("tree glob imports cannot be derived safely", \
        import_glob_indices[index_])
      return
    }
  }
}

function parse_use_group(position, prefix) {
  position++
  while (position <= token_count && tokens[position] != "}") {
    position = parse_use_tree(position, prefix)
    if (failed) {
      return position
    }
    if (tokens[position] == ",") {
      position++
    } else if (tokens[position] != "}") {
      parse_error("unsupported use-tree group", position)
      return position
    }
  }
  if (tokens[position] != "}") {
    parse_error("unterminated use-tree group", position - 1)
    return position
  }
  return position + 1
}

function parse_use_tree(position, prefix, segment, path, alias) {
  if (tokens[position] == "::") {
    position++
  }
  if (tokens[position] == "{") {
    return parse_use_group(position, prefix)
  }
  if (tokens[position] == "*") {
    record_import_glob(prefix, position)
    return position + 1
  }
  if (!is_identifier(tokens[position])) {
    parse_error("unsupported use-tree token: " tokens[position], position)
    return position + 1
  }

  segment = tokens[position]
  path = append_path(prefix, segment)
  position++

  if (tokens[position] == "as") {
    alias = tokens[position + 1]
    if (!is_identifier(alias)) {
      parse_error("unsupported use-tree alias", position)
      return position + 1
    }
    record_import_binding(path, alias, position + 1)
    return position + 2
  }
  if (tokens[position] == "::") {
    return parse_use_tree(position + 1, path)
  }

  record_import_binding(path, "", position - 1)
  return position
}

function derive_lib_bindings(index_, position, count, names, name) {
  for (index_ = 1; index_ <= token_count; index_++) {
    if (tokens[index_] == "use") {
      # A use item inside a nested module (for example a future
      # `pub mod prelude { pub use crate::tree::System; }`) can re-export a
      # tree name the crate-root derivation would never see; fail closed.
      if (brace_depth_at[index_] != 0) {
        parse_error("nested use items cannot participate in the crate-root " \
          "derivation", index_)
        return
      }
      position = parse_use_tree(index_ + 1, "")
      if (failed) {
        return
      }
      if (tokens[position] != ";") {
        parse_error("unsupported crate-root use statement", position)
        return
      }
      index_ = position
      continue
    }
    # `extern crate self as name` is one more crate-root alias spelling; feed
    # it into the fixed-point resolution like `use crate as name`.
    if (tokens[index_] == "extern" && tokens[index_ + 1] == "crate" \
        && tokens[index_ + 2] == "self" && tokens[index_ + 3] == "as" \
        && is_identifier(tokens[index_ + 4])) {
      record_import_binding("crate", tokens[index_ + 4], index_ + 4)
      index_ += 4
      continue
    }
    # A crate-root type alias such as `pub type Renamed = tree::System;` is a
    # re-export the use-tree derivation cannot see; record it for the
    # post-resolution rejection below.
    if (tokens[index_] == "type" && brace_depth_at[index_] == 0) {
      type_alias_count++
      type_alias_starts[type_alias_count] = index_
      position = index_ + 1
      while (position <= token_count && tokens[position] != ";") {
        position++
      }
      type_alias_ends[type_alias_count] = position
      index_ = position
    }
  }

  resolve_tree_bindings()
  if (failed) {
    return
  }

  reject_tree_type_aliases()
  if (failed) {
    return
  }

  count = asorti(tree_bindings, names)
  for (index_ = 1; index_ <= count; index_++) {
    name = names[index_]
    print name
  }
}

function reject_tree_type_aliases(index_, position, token_) {
  for (index_ = 1; index_ <= type_alias_count; index_++) {
    for (position = type_alias_starts[index_]; \
         position <= type_alias_ends[index_]; position++) {
      token_ = tokens[position]
      if (token_ == "tree" || (token_ in tree_bindings)) {
        parse_error("crate-root type aliases over tree exports cannot be " \
          "derived", position)
        return
      }
    }
  }
}

function root_path_at(index_, required_depth, position, supers) {
  if (tokens[index_] == "crate") {
    root_after = index_ + 1
    return 1
  }
  if (tokens[index_] != "super") {
    return 0
  }

  position = index_
  supers = 1
  while (tokens[position + 1] == "::" && tokens[position + 2] == "super") {
    supers++
    position += 2
  }
  if (supers != required_depth) {
    return 0
  }
  root_after = position + 1
  return 1
}

function emit_finding(kind, index_, detail, key) {
  key = kind SUBSEP token_lines[index_] SUBSEP detail
  if (findings_seen[key]) {
    return
  }
  findings_seen[key] = 1
  print kind "\t" token_lines[index_] "\t" detail
}

# Resolve one parsed use-tree path against the module depth of its use
# statement. Returns 1 when the path reaches the crate root, leaving any
# remaining segments in lower_root_rest. Parsing resolves each group item to
# a full path first, so a `super` chain continued inside a nested group
# (`use super::{super::System}`) counts like the flat spelling.
function lower_path_reaches_root(path, count, parts, supers, cursor) {
  count = split(path, parts, "/")
  cursor = 1
  if (parts[1] == "crate") {
    cursor = 2
  } else {
    supers = 0
    while (cursor <= count && parts[cursor] == "super") {
      supers++
      cursor++
    }
    # Fewer `super` segments than the module depth stays below the crate
    # root. More than the depth cannot compile, so treat it as reaching the
    # root and let the segment inspection fail closed.
    if (supers == 0 || supers < lower_use_depth) {
      return 0
    }
  }

  lower_root_rest = ""
  while (cursor <= count) {
    lower_root_rest = append_path(lower_root_rest, parts[cursor])
    cursor++
  }
  return 1
}

function inspect_lower_use_item(path, is_glob, index_, parts) {
  if (!lower_path_reaches_root(path)) {
    return
  }
  if (lower_root_rest == "") {
    if (is_glob) {
      emit_finding("glob", index_, "crate-root glob import")
    } else {
      emit_finding("alias", index_, "crate-root alias")
    }
    return
  }

  split(lower_root_rest, parts, "/")
  if (parts[1] == "self") {
    emit_finding("alias", index_, "crate-root self import or alias")
  } else if (parts[1] == "driver" || parts[1] == "tree") {
    emit_finding("module", index_, "crate-root " parts[1] " import")
  } else if (forbidden_names[parts[1]]) {
    emit_finding("export", index_, "crate-root tree export " parts[1])
  }
}

function scan_lower_uses(index_, position, cursor) {
  for (index_ = 1; index_ <= token_count; index_++) {
    if (tokens[index_] != "use") {
      continue
    }
    # A use statement cannot contain a module declaration, so its module
    # depth is uniform; capture it for the per-item path resolution.
    lower_use_depth = module_depth_at[index_]
    position = parse_use_tree(index_ + 1, "")
    if (failed) {
      return
    }
    if (tokens[position] != ";") {
      parse_error("unsupported use statement", position)
      return
    }
    for (cursor = index_; cursor <= position; cursor++) {
      in_use_statement[cursor] = 1
    }
    index_ = position
  }
}

# Inspect a crate-root path outside a use statement: an expression, type
# position, or cast such as `crate::System::new()`.
function inspect_root_reference(index_, position, token_) {
  position = root_after
  if (tokens[position] == "as") {
    emit_finding("alias", index_, "crate-root alias")
    return
  }
  if (tokens[position] != "::") {
    return
  }

  position++
  token_ = tokens[position]
  if (token_ == "{") {
    parse_error("crate-root use group outside a use statement", index_)
  } else if (token_ == "*") {
    emit_finding("glob", index_, "crate-root glob import")
  } else if (token_ == "driver" || token_ == "tree") {
    emit_finding("module", index_, "crate-root " token_ " reference")
  } else if (forbidden_names[token_]) {
    emit_finding("export", index_, "crate-root tree export " token_)
  }
}

function find_lower_violations(index_, names_count, names) {
  names_count = split(forbidden_exports, names, /\|/)
  for (index_ = 1; index_ <= names_count; index_++) {
    if (names[index_] != "") {
      forbidden_names[names[index_]] = 1
    }
  }

  # Use statements get the full recursive use-tree parse so nested groups
  # and grouped `super` continuations resolve to complete per-item paths.
  scan_lower_uses()
  if (failed) {
    return
  }

  # Everything else is scanned token-by-token for crate-root paths.
  for (index_ = 1; index_ <= token_count; index_++) {
    if (in_use_statement[index_]) {
      continue
    }
    if (tokens[index_] == "extern" && tokens[index_ + 1] == "crate" \
        && tokens[index_ + 2] == "self" && tokens[index_ + 3] == "as" \
        && is_identifier(tokens[index_ + 4])) {
      emit_finding("alias", index_, "extern crate self alias")
    }
    if (root_path_at(index_, module_depth_at[index_])) {
      inspect_root_reference(index_)
    }
  }
}

{
  tokenize($0, FNR)
}

END {
  if (block_comment_depth > 0) {
    parse_error("unterminated block comment", 0)
  }
  if (raw_string_end != "" || in_string) {
    parse_error("unterminated string literal", 0)
  }

  build_depths()
  if (!failed) {
    if (mode == "lib") {
      derive_lib_bindings()
    } else if (mode == "lower") {
      find_lower_violations()
    } else {
      parse_error("unknown checker mode: " mode, 0)
    }
  }

  if (failed) {
    exit 2
  }
}
