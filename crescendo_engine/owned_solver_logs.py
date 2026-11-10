"""Open owned solver logs; ALPS companion aliases have one narrow exception.

This module grants read access only. Its metadata never proves a successful
simulation or that a remote process has stopped.
"""
import contextlib
import math
import os
import stat

from .native_log_io import native_log_read_scope


class OwnedSolverLogError(ValueError):
    def __init__(self, reason):
        self.reason = reason
        ValueError.__init__(self, reason)


def _under(path, root):
    return path.startswith(root + os.sep)


def _directory_flags():
    return (os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
            | getattr(os, 'O_CLOEXEC', 0))


def _open_directory(path, base=None):
    """Walk each component with NOFOLLOW, including ancestors of the root."""
    parts = path.split(os.sep)
    current = os.dup(base) if base is not None else os.open(os.sep, _directory_flags())
    try:
        for part in parts:
            if not part:
                continue
            if part in ('.', '..'):
                raise OwnedSolverLogError('unowned_or_linked_path')
            try:
                following = os.open(part, _directory_flags(), dir_fd=current)
            except FileNotFoundError:
                raise
            except OSError:
                raise OwnedSolverLogError('unowned_or_linked_path')
            os.close(current)
            current = following
        result, current = current, None
        return result
    finally:
        if current is not None:
            os.close(current)


def _regular(info):
    return stat.S_ISREG(info.st_mode) and info.st_nlink == 1


def _fresh(info, launch_epoch):
    if launch_epoch is None:
        return
    if (isinstance(launch_epoch, bool) or not math.isfinite(float(launch_epoch))
            or info.st_mtime + 2 < float(launch_epoch)):
        raise OwnedSolverLogError('stale_log')


def _alias_target(path, link, root, task):
    psf = (task or {}).get('psf_dir')
    if ((task or {}).get('execution_simulator') != 'alps'
            or not isinstance(psf, str) or not os.path.isabs(psf)):
        raise OwnedSolverLogError('unowned_or_linked_path')
    psf = os.path.abspath(psf)
    task_root = os.path.dirname(psf)
    if (path not in (psf + '.log', psf + '.warn') or not _under(psf, root)
            or (task or {}).get('task_root', task_root) != task_root):
        raise OwnedSolverLogError('unowned_or_linked_path')
    target = os.path.abspath(link if os.path.isabs(link) else os.path.join(os.path.dirname(path), link))
    if not _under(target, psf):
        raise OwnedSolverLogError('unowned_or_linked_path')
    return target


@contextlib.contextmanager
def owned_solver_log_reader(path, owned_root, launch_epoch=None, task=None):
    """Yield (fd, metadata), and recheck the alias before closing the reader.

    Ordinary files keep the NOFOLLOW/singly-linked contract. Only an ALPS
    ``<psf>.log`` or ``<psf>.warn`` leaf link may point to a fresh regular file
    inside that exact PSF. Link ancestors, further links and cross-task targets
    are never followed. The target timestamp, not the alias timestamp, decides
    freshness. No descriptor escapes the local native-log cleanup barrier.
    """
    if not isinstance(path, str) or not path or not isinstance(owned_root, str):
        raise OwnedSolverLogError('invalid_path')
    root, source = os.path.abspath(owned_root), os.path.abspath(path)
    if not _under(source, root) or root == os.sep:
        raise OwnedSolverLogError('unowned_or_linked_path')
    root_fd = source_parent = target_parent = opened = None
    with native_log_read_scope():
        try:
            root_fd = _open_directory(root)
            source_parent = _open_directory(os.path.relpath(os.path.dirname(source), root), root_fd) if os.path.dirname(source) != root else os.dup(root_fd)
            name = os.path.basename(source)
            alias = os.stat(name, dir_fd=source_parent, follow_symlinks=False)
            leaf_link = stat.S_ISLNK(alias.st_mode)
            link = None
            resolved = source
            if leaf_link:
                if alias.st_nlink != 1:
                    raise OwnedSolverLogError('linked_or_nonregular_log')
                link = os.readlink(name, dir_fd=source_parent)
                resolved = _alias_target(source, link, root, task)
                if launch_epoch is None:
                    raise OwnedSolverLogError('companion_freshness_unavailable')
                target_parent = _open_directory(os.path.relpath(os.path.dirname(resolved), root), root_fd)
            else:
                target_parent = os.dup(source_parent)
            target_name = os.path.basename(resolved)
            before = os.stat(target_name, dir_fd=target_parent, follow_symlinks=False)
            if not _regular(before):
                raise OwnedSolverLogError('linked_or_nonregular_log')
            _fresh(before, launch_epoch)
            flags = os.O_RDONLY | os.O_NOFOLLOW | getattr(os, 'O_NONBLOCK', 0) | getattr(os, 'O_CLOEXEC', 0)
            try:
                opened = os.open(target_name, flags, dir_fd=target_parent)
            except FileNotFoundError:
                raise
            except OSError:
                raise OwnedSolverLogError('log_changed_during_open')
            info = os.fstat(opened)
            if not _regular(info) or not os.path.samestat(before, info):
                raise OwnedSolverLogError('log_changed_during_open')
            _fresh(info, launch_epoch)
            metadata = {'source_path': source, 'resolved_source_path': resolved,
                        'leaf_link': leaf_link, 'bytes': info.st_size, 'mtime': info.st_mtime,
                        'source_device': alias.st_dev, 'source_inode': alias.st_ino,
                        'target_device': info.st_dev, 'target_inode': info.st_ino}
            yield opened, metadata
            final = os.stat(name, dir_fd=source_parent, follow_symlinks=False)
            if (not os.path.samestat(alias, final) or alias.st_mode != final.st_mode
                    or (leaf_link and os.readlink(name, dir_fd=source_parent) != link)):
                raise OwnedSolverLogError('log_alias_changed_during_read')
            target_final = os.stat(target_name, dir_fd=target_parent, follow_symlinks=False)
            if not _regular(target_final) or not os.path.samestat(info, target_final):
                raise OwnedSolverLogError('log_target_changed_during_read')
        finally:
            for descriptor in (opened, target_parent, source_parent, root_fd):
                if descriptor is not None:
                    os.close(descriptor)
