import json
from .errors import require
from .values import object_body, integer
from .timebase import seconds


class Frames:
    def __init__(self, assets, blobs, processes, artifacts):
        self.assets, self.blobs = assets, blobs
        self.processes, self.artifacts = processes, artifacts

    def find_sheet(self, asset_id, descriptor):
        """Return an existing, fully committed sheet for the same layout.

        Reuse is based on the published artifact rows only, never on an
        in-progress derivation run, so a half-finished request can never be
        handed back as a completed contact sheet.
        """
        with self.artifacts.db.read() as db:
            rows = db.execute(
                "SELECT * FROM artifacts WHERE asset=? AND kind='contact_sheet' ORDER BY created DESC,id DESC",
                (asset_id,)).fetchall()
            for row in rows:
                metadata = json.loads(row["metadata"])
                if all(metadata.get(field) == value for field, value in descriptor.items()):
                    return self.artifacts.get(row["id"], db)
        return None

    def contact_sheet(self, asset_id, raw):
        object_body(raw, ("count", "columns", "cell_width"))
        count = integer(raw.get("count", 9), "count", 1, 36)
        columns = integer(raw.get("columns", 3), "columns", 1, 6)
        cell_width = integer(raw.get("cell_width", 240), "cell_width", 64, 640)
        require(count >= columns, "sheet_layout", "Column count exceeds frame count")
        asset = self.assets.get(asset_id, ready=True)
        require(asset["metadata"]["has_video"], "video_required", "Contact sheets require video")
        duration = seconds(asset["metadata"]["duration_tick"])
        rows = (count+columns-1)//columns
        interval = duration/count
        timestamps = [round((i+.5)*interval, 6) for i in range(count)]
        # The descriptor stays local to this request. A shared attribute would
        # let concurrent sheets of different layouts swap each other's frame
        # counts and sampling times while ffmpeg is running.
        descriptor = {"count": count, "columns": columns, "rows": rows,
                      "cell_width": cell_width, "sample_seconds": timestamps}
        cached = self.find_sheet(asset_id, descriptor)
        if cached is not None:
            return cached
        job = self.artifacts.db.begin_derivation(asset_id, "contact_sheet", descriptor)
        try:
            with self.blobs.temporary(".jpg") as output:
                filter_spec = (f'fps=fps={count}/{duration}:start_time={interval/2},'
                               f'scale={cell_width}:-2,tile={columns}x{rows}:nb_frames={count}:padding=4:margin=4')
                self.processes.run(["-protocol_whitelist", "file,pipe", "-i", str(self.blobs.resolve(asset["blob"])),
                                    "-an", "-vf", filter_spec, "-frames:v", "1", "-q:v", "3", str(output)])
                require(output.is_file(), "empty_sheet", "No video frames were decoded")
                result = self.artifacts.add(asset_id, "contact_sheet", output,
                                          {**descriptor,
                                           "source_blob": asset["blob"], "mime": "image/jpeg"})
                return self.artifacts.db.finish_derivation(job, result)
        except BaseException:
            self.artifacts.db.fail_derivation(job)
            raise
