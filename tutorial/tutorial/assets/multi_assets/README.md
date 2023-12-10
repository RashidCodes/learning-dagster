# Multi-Assets 
A multi-asset represents a set of software-defined assets that are all updated by the same **op** or **graph**.


# When should you use multi-assets 
Multi-assets may be useful in the following scenerios.

- A single call to an API results in multiple tables being updated (e.g. Airbyte, Fivetran, dbt).
- The same in-memory object is used to compute multiple assets.