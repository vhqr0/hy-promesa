import hy

_bootstrapped = False

def _bootstrap(force=False):
    global _bootstrapped
    if force or not _bootstrapped:
        import builtins
        import hy_promesa.core as p
        builtins.__dict__['p'] = p
        builtins._hy_macros['p.mlet'] = p._hy_macros['mlet']
        _bootstrapped = True
