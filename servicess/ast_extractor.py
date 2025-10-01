from enum import Enum
from pathlib import Path
from typing import Generator, Dict

import tree_sitter_c as tsc
import tree_sitter_c_sharp as tscsharp
import tree_sitter_cpp as tscpp
import tree_sitter_javascript as tsjavascript
import tree_sitter_python as tspython
from tree_sitter import Language, Parser, Tree, Node
from tree_sitter_typescript import language_typescript as tstypescript


class Lang(Enum):
    PYTHON = "python"
    # JAVA = "java"
    CPP = "cpp"
    C = "c"
    CSHARP = "c#"
    JAVASCRIPT = "javascript"
    TYPESCRIPT = "typescript"


# TODO: make sure that ext name is compared in lowercase
ext_to_lang: Dict[str, Lang] = {"py": Lang.PYTHON, # "java": Lang.JAVA,
                                "h": Lang.CPP, "cc": Lang.CPP, # use CPP language grammar for C files,
                                # as current C grammar is incompatible with latest version
                                "c": Lang.CPP, "cs": Lang.CSHARP, "cpp": Lang.CPP, "cxx": Lang.CPP, "hxx": Lang.CPP,
                                "js": Lang.JAVASCRIPT, "mjs": Lang.JAVASCRIPT, "ts": Lang.TYPESCRIPT}

Languages: dict[Lang, Language] = {Lang.PYTHON: Language(tspython.language()), # Lang.JAVA: Language(tsjava.language()),
                                   Lang.CPP: Language(tscpp.language()), Lang.C: Language(tsc.language()),
                                   Lang.CSHARP: Language(tscsharp.language()),
                                   Lang.JAVASCRIPT: Language(tsjavascript.language()),
                                   Lang.TYPESCRIPT: Language(tstypescript())}


def extract_text_from_all_nodes(match):
    res = {}
    for [key, value] in match.items():
        res[key] = value[0].text.decode("utf-8")
    return res


def render_if_exists(object, field, prefix="", postfix="", orString="", render_field=lambda x: x):
    return prefix + render_field(object[field]) + postfix if field in object else orString


Queries = {"python": {"class_field": {"handler": (lambda match: {**(m := extract_text_from_all_nodes(match)),
                                                                 "embedding": f"Class field: {render_if_exists(m, "class.instance_field", "[instance] ", render_field=lambda x: "")}{m["class.name"]}.{m["class.field"].replace('self.', '')}"}),
                                      "query": """
            (class_definition
                name: (identifier) @class.name
                body: (block
                    [ 
                        (expression_statement 
                            [
                                (assignment 
                                    left: (identifier) @field.name
                                    right: (_)? @field.default) @class.field
                                (assignment
                                    left: (_
                                        name: (identifier) @field.name
                                        type: (_) @field.type)
                                    right: (_)? @field.default) @class.field
                                (_
                                    name: (identifier) @field.name
                                    type: (_) @field.type) @class.field
                            ]
                        )
                        (function_definition
                            (identifier) @method.name
                            (#match @method.name "^__init__$")
                            (parameters) @method.parameters
                            (block
                                (expression_statement) @class.field @class.instance_field
                                (#match @class.instance_field "^self\\.")
                            )
                        )
                    ]
                )
            )
        """}, "class_method": {"handler": (lambda match: {**(m := extract_text_from_all_nodes(match)),
                                                          "embedding": f"Class method: {render_if_exists(m, "method.decorator", "[", "] ")}{m["class.name"]}.{m["method.name"]}{m["method.parameters"]}{render_if_exists(m, "method.type", " -> ")}"}),
                               "query": """
            (class_definition
                name: (identifier) @class.name
                body: (block 
                    [
                        (function_definition
                            name: (identifier) @method.name
                            parameters: (parameters) @method.parameters
                            (type)? @method.type
                        )
                        (decorated_definition
                            (decorator (_) @method.decorator) 
                            (function_definition
                                name: (identifier) @method.name
                                parameters: (parameters) @method.parameters
                                (type)? @method.type
                            )
                        )
                    ]                        
                )
            ) ;; @class_method
        """}, "class": {"handler": (lambda match: {**(m := extract_text_from_all_nodes(match)),
                                                   "embedding": f"Class: {m["class.name"]}{render_if_exists(m, "class.base")}"}),
                        "query": """
            (class_definition
                name: (identifier) @class.name
                superclasses: (argument_list)? @class.base
            ) ;; @class
        """}, "function": {"handler": (lambda match: {**(m := extract_text_from_all_nodes(match)),
                                                      "embedding": f"Function: {m["function.name"]}{m["function.parameters"]}{render_if_exists(m, "function.type", " -> ")}"}),
                           "query": """
            (module
                (function_definition
                    name: (identifier) @function.name
                    parameters: (parameters) @function.parameters
                    (type)? @function.type
                    (comment)? @function.docstring
                ) ;; @function
            )
        """}, "module_type": {"handler": (lambda match: {**(m := extract_text_from_all_nodes(match)),
                                                         "embedding": f"Type: {m['type.name']} = {m['type.value']}"}),
                              "query": """
            (module
                (type_alias_statement
                    (type (identifier) @type.name)
                    (type (generic_type) @type.value)
                ) @type
            )
        """}, "constant": {
    "handler": (lambda match: {**(m := extract_text_from_all_nodes(match)), "embedding": f"Constant: {m["constant"]}"}),
    "query": """
            (module
                (expression_statement
                    (assignment
                        left: (identifier) @constant.name
                        right: (_) @constant.value
                    ) @constant
                )
            )
        """}, "import": {"handler": (lambda match: {**(m := extract_text_from_all_nodes(match)),
                                                    "embedding": f"Import: {m["import.name"]}{render_if_exists(m, "import.from", " from ")}"}),
                         "query": """
            (module
                (import_from_statement
                    module_name: (dotted_name) @import.from
                    (#match @import.from "^\\\\w+$")
                    name: (dotted_name)? @import.name
                    (aliased_import (dotted_name) @import.name)?
                ) @import
            )
            (module
                (import_statement
                    (dotted_name) @import.name
                ) @import
            )
            (module
                (import_statement
                    (aliased_import (dotted_name) @import.name)
                ) @import
            )
        """}, "local_import": {"handler": (lambda match: {**(m := extract_text_from_all_nodes(match)),
                                                          "embedding": f"Import: {m["import.name"]} from {m["import.from"]}"}),
                               "query": """
            (module
                (import_from_statement
                    (dotted_name
                        (identifier)
                        ("." (identifier))+
                    ) @import.from @import.path
                    name: (dotted_name)? @import.name
                    (aliased_import (dotted_name) @import.name)?
                ) @import
            )
        """}}}


def get_file_params(file_path: str) -> [str, str]:
    path = Path(file_path)
    return path.name, path.suffix.lower()


def read_file(file_path: str):
    name, extension = get_file_params(file_path)
    lang = get_language(extension)
    with open(file_path, "rb") as f:
        return f.read(), name, lang


def get_language(extension: str):
    return ext_to_lang[extension[1:]]


def parse_code(code: bytes, lang: Lang):
    parser = Parser(Languages[lang])
    return parser.parse(code)


def traverse_tree(tree: Tree) -> Generator[Node, None, None]:
    cursor = tree.walk()

    visited_children = False
    level = 0
    while True:
        if not visited_children:
            yield cursor.node, level
            if cursor.goto_first_child():
                level += 1
            else:
                visited_children = True
        elif cursor.goto_next_sibling():
            visited_children = False
        elif cursor.goto_parent():
            level -= 1
        else:
            break


def extract_tree(tree: Tree, file):
    with open(file, "w") as f:
        for node, level in traverse_tree(tree):
            f.write(f"{'  ' * level}{node.type}: {node.text}\n")


def ast_main_definitions_iterator(lang, tree) -> Generator[dict, None, None]:
    for [query_name, config] in Queries[lang].items():
        handler, query_pattern = config["handler"], config["query"]
        # print(f"\nQuery: {query_name}, Pattern: {query_pattern}")
        print(f"\nQuery: {query_name}")
        query = Languages[lang].query(query_pattern)
        for [_, capture] in query.matches(tree.root_node):
            yield dict(element_type=query_name, **handler(capture))


def ast_iterator(lang, tree, query) -> Generator[tuple[int, dict[str, list[Node]]], None, None]:
    assert query is not None, "Query should be provided"
    try:
        tree_sitter_query = Languages[lang].query(query)
        for match in tree_sitter_query.matches(tree.root_node):
            yield match
    except Exception as e:
        print(f"Error while parsing {lang} code: {query}, {e=}")
        return


lang_to_comment_query_map: Dict[Lang, str] = {Lang.PYTHON: """
        (
          (string) @docstring
          (#match? @docstring "^(\\"\\"\\"|''')")
        )
        (
          (comment)+ @comment
        )
    """,

    Lang.CPP: """
        (
          (comment)+ @comment
        )
        (
          (raw_string_literal) @docstring
        )
    """,

    Lang.C: """
        (
          (comment)+ @comment
        )
        (
          (raw_string_literal) @docstring
        )
    """,

    # Lang.JAVA: """
    #     (
    #       (block_comment) @comment
    #     )
    #     (
    #       (line_comment)+ @comment
    #     )
    #     (
    #       (javadoc) @docstring
    #     )
    # """,

    Lang.JAVASCRIPT: """
        (
          (comment)+ @comment
        )
    """,

    Lang.CSHARP: """
    (
      (comment)+ @comment
    )
""",

    Lang.TYPESCRIPT: """
    (
      (comment)+ @comment
    )
    (
      (jsdoc) @docstring
    )
""",

}

COMMENT_SYMBOLS = " *#'\"/-_="


def transform_text(text: str):
    return " ".join([part.lstrip(COMMENT_SYMBOLS) for part in text.split("\n") if part]).rstrip(
        COMMENT_SYMBOLS).removeprefix("\n ")


def extract_text_from_comments_node(match) -> Generator[str, None, None]:
    for [_, value] in match.items():
        yield " ".join([transform_text(v.text.decode("utf-8")) for v in value]).strip()


def extract_comments(lang, tree) -> Generator[str, None, None]:
    for [_, match] in ast_iterator(lang, tree, lang_to_comment_query_map[lang]):
        yield from extract_text_from_comments_node(match)


def code_comments_iterator(file_path: str) -> Generator[str, None, None]:
    code, filename, lang = read_file(str(file_path))
    tree = parse_code(code, lang)
    yield from extract_comments(lang, tree)
