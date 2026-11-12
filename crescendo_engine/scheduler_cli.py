"""User-configured argv for the scheduler key-value protocol.

No executable, option layout or site profile is inferred. Templates are split
before expansion, and query/cancel always bind one exact owned numeric Job ID.
"""
import os
import shutil

from crescendo_engine.runtime import bind_text
_contract_text = bind_text()

from .execution_commands import ExecutionCommandError, _render, scheduler_template_argv
from .scheduler_protocol import valid_job_id

SCHEMA = _contract_text('crescendo.scheduler_templates.v1')
TEMPLATE_KEYS = ('scheduler_query_template', 'scheduler_cancel_template')


def scheduler_capability(commands, context=None):
    from .scheduler_providers import capability
    return capability(commands, context)


def scheduler_command_argv(template, job_id, values=None):
    if not valid_job_id(job_id):
        raise ExecutionCommandError('Invalid exact scheduler Job ID')
    scheduler_template_argv(template)
    return _render(template, dict(values or {}, job_id=str(job_id)))


def resolve_scheduler_binary(executable, environment=None):
    """Resolve only the executable explicitly named by a user template."""
    environment = os.environ if environment is None else environment
    if os.path.isabs(executable):
        candidate = executable
    else:
        candidate = shutil.which(executable, path=environment.get('PATH', ''))
    if candidate and os.path.isfile(candidate) and os.access(candidate, os.X_OK):
        return os.path.abspath(candidate)
    return None
