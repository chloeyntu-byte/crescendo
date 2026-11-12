"""Startup example: register before importing execution consumers.

The fictional provider uses the configured generic templates and key-value
protocol. It cannot change the engine's receipt, Stop or terminal criteria.
"""
from crescendo_engine.scheduler_providers import TemplateProvider, register_provider


class FictionalProvider(TemplateProvider):
    version = 'fictional-1'


def register():
    register_provider('fictional', FictionalProvider())
