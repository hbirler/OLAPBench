import string


class Template(string.Template):
    idpattern = r"""(?a:[_.a-z][_.a-z0-9]*)"""
