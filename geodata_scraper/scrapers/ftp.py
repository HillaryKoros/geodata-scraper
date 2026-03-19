"""
FTP/SFTP scraper — download geodata from FTP servers.
"""

import ftplib
import logging
from pathlib import Path

from .base import BaseScraper, ExtractResult

log = logging.getLogger("geodata_scraper.ftp")


class FTPScraper(BaseScraper):
    name = "ftp"
    description = "FTP/SFTP geodata scraper"

    def __init__(
        self,
        host: str = "",
        port: int = 21,
        username: str = "anonymous",
        password: str = "",
        remote_paths: list[str] | None = None,
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.remote_paths = remote_paths or []

    def build_tasks(self, countries: list[str] = None, **kwargs) -> list[dict]:
        storage_dir = kwargs.get("storage_dir", Path("/tmp/geodata_scraper"))
        tasks = []

        for rpath in self.remote_paths:
            filename = rpath.split("/")[-1]
            dest = storage_dir / "ftp" / self.host / filename
            fmt = Path(filename).suffix.lstrip(".")
            tasks.append(
                {
                    "url": f"ftp://{self.host}{rpath}",
                    "dest": dest,
                    "remote_path": rpath,
                    "iso3": "",
                    "admin_level": -1,
                    "format": fmt,
                }
            )

        return tasks

    def extract(self, task: dict, storage_dir: Path) -> ExtractResult:
        dest = task["dest"]
        rpath = task["remote_path"]
        dest.parent.mkdir(parents=True, exist_ok=True)

        if dest.exists() and dest.stat().st_size > 0:
            return ExtractResult(
                url=task["url"],
                local_path=dest,
                iso3="",
                admin_level=-1,
                format=task["format"],
                size=dest.stat().st_size,
                success=True,
            )

        try:
            ftp = ftplib.FTP()
            ftp.connect(self.host, self.port)
            ftp.login(self.username, self.password)

            _size = ftp.size(rpath) or 0  # noqa: F841
            tmp = dest.with_suffix(dest.suffix + ".part")

            with open(tmp, "wb") as f:
                ftp.retrbinary(f"RETR {rpath}", f.write)

            ftp.quit()
            tmp.rename(dest)

            log.info(f"OK: {dest.name} ({dest.stat().st_size} bytes)")
            return ExtractResult(
                url=task["url"],
                local_path=dest,
                iso3="",
                admin_level=-1,
                format=task["format"],
                size=dest.stat().st_size,
                success=True,
            )

        except (ftplib.all_errors, OSError) as e:
            log.error(f"FAILED: {dest.name} — {e}")
            return ExtractResult(
                url=task["url"],
                local_path=dest,
                iso3="",
                admin_level=-1,
                format=task["format"],
                size=0,
                success=False,
                error=str(e),
            )
