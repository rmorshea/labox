# Storables

Storables define the shape of the data that's saved and loaded by Lakery. Try these
implementations:

- [Pydantic models](../integrations/3rd-party/pydantic.md)
- [Built-ins](../integrations/built-ins/storables.md)

## Defining Storables

To define a storable class you need to inherit from
[`Storable`][lakery.core.storable.Storable] and provide

- [`class_id`](#class-ids) - a string that uniquely and permanently identifies the
    class.
- [`Unpacker`](./unpackers.md) - a class that defines how to destructure the class
    into its constituent parts.

```python
from lakery import Storable
from lakery import Unpacker

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
associated with the class so each `Storable` and `Unpacker` pairing and/or
implementation must document what types of changes are compatible and which are not.

In general, you should avoid:

- Removing fields from the class.
- Changing the expected type of fields (though converting or adding to a union type is
    generally safe).
- Renaming fields if there is no way to provide an alias for the old name.

If you must make a change that is not compatible with existing data, you should create a
new `Storable` class with a new `class_id`. This will ensure that existing data remains
accessible while allowing you to create new data with the updated.

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

### Generating IDs

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

### Class ID Format

The `class_id` should be a unique, 8-32 character hexadecimal string. Ultimately the
`class_id` is normalized as a UUID so it can be used in a variety of contexts, such as
URLs or database keys. This means that class ID's with less than 32 characters will be
padded with zeroes until it is the standard 16 bytes long.
