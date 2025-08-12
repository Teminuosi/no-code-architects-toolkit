# Copyright (c) 2025 Stephen G. Pope
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.



from flask import Blueprint
from app_utils import *
import logging
from services.v1.video.split import split_video
from services.authentication import authenticate

v1_video_split_bp = Blueprint('v1_video_split', __name__)
logger = logging.getLogger(__name__)

@v1_video_split_bp.route('/v1/video/split', methods=['POST'])
@authenticate
@validate_payload({
    "type": "object",
    "properties": {
        "video_url": {"type": "string", "format": "uri"},
        "splits": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "start": {"type": "string"},
                    "end": {"type": "string"}
                },
                "required": ["start", "end"],
                "additionalProperties": False
            },
            "minItems": 1
        },
        "video_codec": {"type": "string"},
        "video_preset": {"type": "string"},
        "video_crf": {"type": "number", "minimum": 0, "maximum": 51},
        "audio_codec": {"type": "string"},
        "audio_bitrate": {"type": "string"},
        "webhook_url": {"type": "string", "format": "uri"},
        "id": {"type": "string"}
    },
    "required": ["video_url", "splits"],
    "additionalProperties": False
})
@queue_task_wrapper(bypass_queue=False)
def video_split(job_id, data):
    """Split a video file into multiple segments with optional encoding settings."""
    video_url = data['video_url']
    splits = data['splits']
    
    # Extract encoding settings with defaults
    video_codec = data.get('video_codec', 'libx264')
    video_preset = data.get('video_preset', 'medium')
    video_crf = data.get('video_crf', 23)
    audio_codec = data.get('audio_codec', 'aac')
    audio_bitrate = data.get('audio_bitrate', '128k')
    
    logger.info(f"Job {job_id}: Received video split request for {video_url}")
    
    try:
        # Process the video file and get aligned results for each input split
        results, input_filename = split_video(
            video_url=video_url,
            splits=splits,
            job_id=job_id,
            video_codec=video_codec,
            video_preset=video_preset,
            video_crf=video_crf,
            audio_codec=audio_codec,
            audio_bitrate=audio_bitrate
        )
        
        # Upload all success outputs to cloud storage; keep index aligned to input
        from services.cloud_storage import upload_file
        response = []
        import os
        for idx, r in enumerate(results):
            item = {"index": idx, "start": splits[idx]["start"], "end": splits[idx]["end"]}
            if isinstance(r, dict) and r.get("status") == "ok" and r.get("output_file"):
                try:
                    cloud_url = upload_file(r["output_file"])
                    item["file_url"] = cloud_url
                    # best-effort cleanup of local output
                    try:
                        os.remove(r["output_file"])  # remove local file after upload
                        logger.info(f"Job {job_id}: Uploaded and removed split output for index {idx}")
                    except Exception:
                        pass
                except Exception as e:
                    item["error"] = f"Upload failed: {str(e)}"
            else:
                item["error"] = r.get("error") if isinstance(r, dict) else "Unknown error"
            response.append(item)

        # Clean up input file
        try:
            os.remove(input_filename)
            logger.info(f"Job {job_id}: Removed input file")
        except Exception:
            pass

        logger.info(f"Job {job_id}: Video split operation completed successfully with {len(response)} results")
        return response, "/v1/video/split", 200
        
    except Exception as e:
        logger.error(f"Job {job_id}: Error during video split process - {str(e)}")
        return str(e), "/v1/video/split", 500