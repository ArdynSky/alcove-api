# Register Debate extensions before api.main imports the shared Debate router.
from . import debate_audience  # noqa: F401
