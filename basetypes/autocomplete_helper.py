from enum import EnumType


def generate_autocompletion_for_enum(e: EnumType):
    return '### BEGIN AUTO GENERATION\n' \
           f'# Auto-generated from {e.__name__} for auto-completion purpose\n' \
           f"class {e.__name__.replace('Type', '')}(SerializationNode):" \
           f"\n    {'\n    '.join(attr.name + ' = ...' for attr in e)}" \
           '\n### END AUTO GENERATION\n'
