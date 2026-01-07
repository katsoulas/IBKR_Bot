from datetime import datetime
from zoneinfo import ZoneInfo
NY_TZ = ZoneInfo("America/New_York")
def ny_now():
    return datetime.now(NY_TZ)
