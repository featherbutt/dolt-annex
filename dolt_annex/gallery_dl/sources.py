from abc import abstractmethod
from typing_extensions import Any, Dict, Iterable, override

from dolt_annex.datatypes.common import TableRow

class GalleryDLSource:
    """A supported gallery-dl source. Methods describe how to parse and process metadata from that source."""

    @abstractmethod
    def table_key(self, metadata: dict[str, Any]) -> TableRow:
        """The table key used for submissions from this source."""

    
    @abstractmethod
    def fields_to_remove(self) -> list[str]:
        """
        A list of fields that will be removed from the imported metadata.
        
        In general, imported metadata should:
        - Be identical regardless of the endpoint used to fetch it (e.g. search results vs. individual submission)
        - Only change when the submission itself changes (excluding things like view counts)
        """

    def format_file_metadata(self, metadata: dict[str, Any]):
        """Format the metadata in a source-specific way. Can be overridden by implementations."""
        mutate_remove_fields(metadata, self.fields_to_remove())

    def format_post_metadata(self, metadata: dict[str, Any]):
        """Format the metadata in a source-specific way. Can be overridden by implementations."""
        mutate_remove_fields(metadata, self.fields_to_remove())

    def import_post(self, metadata: dict[str, Any]) -> Iterable[tuple[str, TableRow]]:
        """Some sources have extra metadata for 'posts' that contain multiple submissions."""
        return []
        
class Itaku(GalleryDLSource):
    """Support for itaku.ee"""

    @override
    def table_key(self, metadata: dict[str, Any]) -> TableRow:
        return TableRow((
            "itaku.ee",
            metadata["id"],
            metadata.get("date_edited") or metadata.get("date_added"),
        ))

    @override
    def fields_to_remove(self) -> list[str]:
        return [
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
            # Some endpoints only return regular and xl images. Removing large makes the output consistent.
            "image_lg",
            "num_too_mature_imgs",
        ]

    @override
    def format_post_metadata(self, metadata: dict[str, Any]):
        super().format_post_metadata(metadata)

        if (folders := metadata.get("folders")):
            for folder in folders:
                del folder["num_posts"]

        if (gallery_images := metadata.get("gallery_images")):
            for image in gallery_images:
                mutate_remove_fields(image, self.fields_to_remove())

    @override
    def format_file_metadata(self, metadata: dict[str, Any]):
        match metadata.get("subcategory"):
            case "post":
                # Move image metadata to the top level and remove unneeded fields.
                file: dict = metadata.get("file", {})
                mutate_remove_fields(file, self.fields_to_remove())
                for field in list(metadata.keys()):
                    if not field.startswith("_") and field not in ("category", "subcategory", "filename", "extension"):
                        del metadata[field]
                metadata.update(file)
            case _:
                super().format_post_metadata(metadata)

        if (folders := metadata.get("folders")):
            for folder in folders:
                del folder["num_posts"]

    @override
    def import_post(self, metadata: dict[str, Any]) -> Iterable[tuple[str, TableRow]]:
        if metadata.get("subcategory") == "post":
            yield "posts", self.table_key(metadata)

class Furaffinity(GalleryDLSource):
    """Support for furaffinity.net"""
    @override
    def table_key(self, metadata: dict[str, Any]) -> TableRow:
        return TableRow(( "furaffinity.net", metadata["id"], metadata["date"]))

    @override
    def fields_to_remove(self) -> list[str]:
        return [
            "comments",
            "favorites",
            "views"
        ]

category_to_source : Dict[str, GalleryDLSource]= {
    "itaku": Itaku(),
    "furaffinity": Furaffinity(),
}

def get_source(category: str) -> GalleryDLSource:
    if not (source := category_to_source.get(category)):
        raise ValueError(f"Category {category} is not currently supported. Supported categories: {list(category_to_source.keys())}")
    return source


def mutate_remove_fields(d: dict, fields_to_remove: list):
    """Remove fields from a dictionary in place."""
    for field in fields_to_remove:
        if field in d:
            del d[field]