#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Helper functions that gallery-dl postprocessors can use to format data for dolt-annex.
"""

post_fields_to_remove = [
    "liked_by_you",
    "num_likes",
    "reshared_by_you",
    "num_reshares",
    "too_mature",
    "num_too_mature_imgs",
]

image_fields_to_remove = [
    "liked_by_you",
    "num_likes",
    "too_mature",
    "blacklisted",
    "bookmarked_by_you",
    "num_reshares",
    "num_comments",
    "is_thumbnail_for_video",
    "show_content_warning",
    "reshared_by_you",
    "image_lg"
]

def mutate_remove_fields(d: dict, fields_to_remove: list):
    for field in fields_to_remove:
        if field in d:
            del d[field]

def itaku_post_format(post: dict):
    mutate_remove_fields(post, post_fields_to_remove)
    if "gallery_images" in post and (images := post["gallery_images"]) is not None:
        for image in images:
            mutate_remove_fields(image, image_fields_to_remove)
    if "folders" in post and (folders := post["folders"]) is not None:
        for folder in folders:
            del folder["num_posts"]
    
def itaku_post_file_format(post:dict):
    """Move image metadata to the top level and remove unneeded fields."""
    file: dict = post.get("file", {})
    mutate_remove_fields(file, image_fields_to_remove)
    for field in list(post.keys()):
        if field not in ("category", "subcategory", "filename", "extension"):
            del post[field]
    post.update(file)
