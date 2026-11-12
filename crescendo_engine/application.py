"""Explicit presentation selection for embedders and public launchers."""
import collections


class RuntimeServices(collections.namedtuple('_Services',
        'config service launch_context diagnostic_actions force_exit_process')):
    __slots__ = ()

    def __new__(cls, config, service=None, launch_context=None, diagnostic_actions=(),
                force_exit_process=False):
        return super(RuntimeServices, cls).__new__(cls, config, service, launch_context,
                                                  tuple(diagnostic_actions), force_exit_process)


def launch_application(runtime_services, gui_factory=None):
    from .gui import launch_gui, native_gui_factory
    return launch_gui(runtime_services.config, service=runtime_services.service,
                      launch_context=runtime_services.launch_context,
                      force_exit_process=runtime_services.force_exit_process,
                      gui_factory=gui_factory or native_gui_factory,
                      diagnostic_actions=runtime_services.diagnostic_actions)
