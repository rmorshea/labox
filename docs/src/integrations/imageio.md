# ImageIO

!!! note

    Install with `pip install labox[imageio]`

## Media Serializer

The [`MediaSerializer`][labox.extra.imageio.MediaSerializer] provides a
[serializer](../concepts/serializers.md) implementation for various types of media
supported by the [`imageio`][imageio] library. Instead of passing data directly, it
works with [`Media`][labox.extra.imageio.Media] objects that wrap NumPy arrays with
format metadata.

### Basic Usage

A default instance of the serializer is available by importing
[media_serializer][labox.extra.imageio.media_serializer]:

```python
import numpy as np

from labox.extra.imageio import Media
from labox.extra.imageio import media_serializer

# Create media data - can be a single array or sequence of arrays
gen = np.random.Generator(np.random.PCG64())
image_data = gen.integers(0, 255, (100, 100, 3), dtype=np.uint8)
media = Media(data=image_data, extension=".png")

media_serializer.serialize_data(media)
```

You can also create a custom instance with specific MIME type configuration:

```python
from mimetypes import MimeTypes

from labox.extra.imageio import MediaSerializer

custom_mimetypes = MimeTypes()
custom_mimetypes.add_type("image/webp", ".webp")

serializer = MediaSerializer(mimetypes=custom_mimetypes)
```

### Media Data Types

The [`Media`][labox.extra.imageio.Media] class wraps media data with format information:

```python
from labox.extra.imageio import Media

# Single image
image = Media(
    data=image_array,
    extension=".jpg",  # Will determine content type automatically
)

# Video or image sequence
video = Media(
    data=[frame1, frame2, frame3],  # Sequence of arrays
    extension=".mp4",
)

# Using specific plugin instead of extension
media = Media(
    data=image_array,
    plugin="PNG-PIL",  # Use specific ImageIO plugin
)
```

### Format Support

The serializer supports any format that ImageIO can handle, including:

- **Images**: PNG, JPEG, GIF, TIFF, BMP, WebP, and many others
- **Videos**: MP4, AVI, MOV, WebM (when appropriate codecs are available)
- **Scientific formats**: DICOM, FIT, and other specialized formats

Format selection can be controlled in two ways:

### Content Type Detection

The serializer automatically determines the MIME type based on the file extension using
Python's [`mimetypes`][mimetypes] module. If no extension is provided or the type cannot
be determined, it defaults to `"application/octet-stream"`.

### Configuration Persistence

The serializer stores the `plugin` and `extension` information in the serialized data's
[config](../concepts/serializers.md#serializer-config), ensuring that media can be
deserialized using the same format settings that were used during serialization. This is
particularly important for maintaining compatibility when specific plugins or format
options are required.
