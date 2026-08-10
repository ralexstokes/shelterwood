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

function record_tree_binding(path, alias, index_, normalized, binding, parent) {
  normalized = normalized_path(path)
  if (normalized != "tree" && normalized !~ /^tree\//) {
    return
  }

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
    parse_error("unsupported tree import binding: " binding, index_)
    return
  }
  tree_bindings[binding] = 1
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
    if (normalized_path(prefix) == "tree" \
        || normalized_path(prefix) ~ /^tree\//) {
      parse_error("tree glob imports cannot be derived safely", position)
    }
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
    record_tree_binding(path, alias, position + 1)
    return position + 2
  }
  if (tokens[position] == "::") {
    return parse_use_tree(position + 1, path)
  }

  record_tree_binding(path, "", position - 1)
  return position
}

function derive_lib_bindings(index_, position, count, names, name) {
  for (index_ = 1; index_ <= token_count; index_++) {
    if (tokens[index_] != "use" || brace_depth_at[index_] != 0) {
      continue
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
  }

  count = asorti(tree_bindings, names)
  for (index_ = 1; index_ <= count; index_++) {
    name = names[index_]
    print name
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

function inspect_root_group(position, origin, depth, item_start, token_) {
  depth = 1
  item_start = 1
  for (position = position + 1; position <= token_count; position++) {
    token_ = tokens[position]
    if (token_ == "{") {
      depth++
      continue
    }
    if (token_ == "}") {
      depth--
      if (depth == 0) {
        return
      }
      continue
    }
    if (depth != 1) {
      continue
    }
    if (token_ == ",") {
      item_start = 1
      continue
    }
    if (!item_start) {
      continue
    }

    item_start = 0
    if (token_ == "*") {
      emit_finding("glob", origin, "crate-root glob import")
    } else if (token_ == "self") {
      emit_finding("alias", origin, "crate-root self import or alias")
    } else if (token_ == "driver" || token_ == "tree") {
      emit_finding("module", origin, "crate-root " token_ " import")
    } else if (forbidden_names[token_]) {
      emit_finding("export", origin, "crate-root tree export " token_)
    }
  }
  parse_error("unterminated crate-root use group", origin)
}

function inspect_root_path(index_, position, token_) {
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
    inspect_root_group(position, index_)
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

  for (index_ = 1; index_ <= token_count; index_++) {
    if (root_path_at(index_, module_depth_at[index_])) {
      inspect_root_path(index_)
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
