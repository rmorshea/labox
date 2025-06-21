# Storables

Storables define the shape of the data that's saved and loaded by Lakery.

Third party integrations:

-   [`Pydantic`](../integrations/pydantic.md)

Built-in implementations:

-   [`StorableValue`][lakery.builtin.storables.StorableValue] for simple values.
-   [`StorableStream`][lakery.builtin.storables.StorableStream] for streaming data.

## Defining Storables

To define a storable class you need to inherit from
[`Storable`][lakery.core.storable.Storable] and provide

-   A [`class_id`](#class-id) that uniquely and permanently identifies the class.
-   An [`Unpacker`](#unpackers) that defines how to destructure the class into its
    constituent parts.

```python
from lakery import Storable, Unpacker

my_unpacker: Unpacker


class MyStorable(Storable, class_id="abc123", unpacker=my_unpacker):
    """A storable class that can be saved and loaded by Lakery."""
    ...
```

### Backwards Compatibility

When you define a `Storable` class, much like defining a table in an ORM (e.g.
SQLAlchemy) you are creating a contract for how data will be serialized and
deserialized. This means that you must be careful when changing the structure of a
`Storable` class after it has been used to save data.

Ultimately whether a change is or isn't compatible is constrained by the `Unpacker`
associated with the class so each `Storable` and `Unpacker` pairing/implementation must
document what types of changes are compatible and which are not.

In general, you should avoid:

-   Removing fields from the class.
-   Changing the type of fields (though converting or adding to a union type is
    generally safe).
-   Renaming fields if there is no way to provide an alias for the old name.

If you must make a change that is not compatible with existing data, you should create a
new `Storable` class with a new `class_id`. This will ensure that existing data remains
accessible while allowing you to create new data with the updated.

## Unpackers

Unpackers define how to destructure a `Storable` class into its constituent parts as
well as how to reconstitute it from those parts. Unpackers are typically specialized for
a specific set of `Storable` subclasses that share a common framework. For example, all
[Pydantic](../integrations/pydantic.md) classes use the same unpacker, which understand
how to convert Pydantic models into a dictionary of fields and values.

### Defining Unpackers

To define an unpacker you need to first decide what types of `Storable` classes it
should handle. Then you can implement the `Unpacker` interface, which requires you to
provide the following:

-   A `name` that uniquely and permanently identifies the unpacker.
-   An `unpack_object` method that takes a `Storable` instance and returns a dictionary
    of fields with their values as well as where and how to store them.
-   A `repack_object` method that takes a `Storable` class and the aforementioned
    dictionary, and returns a new instance of the `Storable` class.

In the simplest case, this might look something like the following:

```python
from lakery import Unpacker, Storable, UnpackedValue


class MyUnpacker(Unpacker):
    name = "my-unpacker@v1"

    def unpack_object(self, obj, registry):
        return {k: UnpackedValue(value=v) for k, v in obj.__dict__.items()}

    def repack_object(self, cls, contents, registry):
        return cls(**{k: u["value"] for k, u in contents.items()})
```

Then in your `Storable` class you can use this unpacker:

```python
from lakery import Storable


class MyStorable(Storable, class_id="abc123", unpacker=MyUnpacker()):
    ...
```

All subclasses of `MyStorable` will now use the `MyUnpacker` to unpack and repack their
data.

### Unpacked Values

When you unpack a `Storable` class, the values are returned as
[`UnpackedValue`][lakery.core.storable.UnpackedValue] dicts. These values contain the
following information:

-   `value`: The actual value of the field.
-   `serializer` (optional): The serializer to use when saving the value.
-   `storage` (optional): The storage to use when saving the value.

If you do not specify a serializer. One will be inferred based on the type of the value.
If you do not specify a storage, the default storage in the given
[registry](./registries.md) will be used.

### Unpacked Streams

In addition to unpacked values, you can also unpack the fields of a `Storable` class
into `UnpackedValueStream` dicts. This is useful when you can't or don't want to load
large amounts of data into memory at once. An `UnpackedValueStream` contains the
following information:

-   `value_stream`: An async iterable that yields the values of the fields.
-   `serializer` (recommended): The serializer to use when saving the values.
-   `storage` (optional): The storage to use when saving the values.

In this case providing an explicit serializer is recommended because attempting to infer
the appropriate serializer will raise a late error since the first value from the stream
must be inspected. As before, if you do not specify a storage, the default storage in
the [registry](./registries.md) will be used.

Unpacked values and value streams may be mixed within the same unpacked dictionary,
allowing you to unpack some fields into memory while streaming others.

## Class IDs

The `class_id` within the [config][lakery.core.storable.Storable.get_storable_config] of
a `Storable` class uniquely identify it when saving and loading data. This is important
because it's how Lakery knows which class to use when reconstituting data. That means
you should **never copy or change this value** once it's been used to save production
data. On the other hand you are free to rename the class or move it to a different
module without any issues since the `class_id`, rather than an "import path", is what
identifies it.

!!! note

    You may omit a `class_id` if you are defining an abstract class that is not intended
    to be saved or loaded directly.

#### Generating IDs

When defining a `Storable` class you plan to save and load data with you can declare a
placeholder `class_id` value like `"..."`. This is a signal to Lakery that you intend to
generate a unique ID later. Lakery will then issue a warning when you run the code,
prompting you to replace the placeholder with a unique ID it suggests.

```python
from lakery import Storable

class MyStorable(Storable, class_id="..."):
    pass
```

This will generate a warning like:

```txt
MyStorable does not have a valid storable class ID. Got '...'. Consider using 'abc123'.
```

!!! note

    In the future, Lakery will come with a linter that automatically generates unique
    class IDs as you work.

#### Class ID Format

The `class_id` should be a unique, 8-32 character hexadecimal string. Ultimately the
`class_id` is normalized as a UUID so it can be used in a variety of contexts, such as
URLs or database keys. This means that class ID's with less than 32 characters will be
padded with zeroes until it is the standard 16 bytes long.
