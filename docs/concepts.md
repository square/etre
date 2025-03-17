# Etre Concepts

**Etre is a data store and API for tracking and finding resources.**

Those aren't randomly chosen words; each one is meaningful:

* _Data store_: Etre is backed by a _durable_ data store, not a cache
* _API_: Etre is accessed by a simple REST API&mdash;for humans and machines
* _Tracking_: Etre is a source of truth for "what exists" (resources)
* _Finding_: Etre uses the [Kubernetes label selector](https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/) syntax to make finding resources easy
* _Resources_: Etre can store anything but it's built to store resources that actually exist

In large fleets, Etre allows humans and machines to find arbitrary groups of resources.
For example, at [Block](https://github.com/block/) where Etre was created, there are thousands of databases.
When a DBA needs to reboot a certain group of databases, how do they find and select only the databases they need?
They use Etre.

Etre has three main concepts:

* **entity**: A unique resource of a specific type
* **label**: Key-value pairs associated with an entity
* **ID**: An ordered set of labels that uniquely identify an entity

The canonical example is physical servers:

```json
[
    {
        "hostname": "db1.local",
        "model": "Dell R760"
    },
    {
        "hostname": "db2.local",
        "model": "Dell R760",
        "rack": "34a"
    }
]
```

In this example, the user-defined entity type is `host`, and there are 2 host entities.

Each host entity has 2 labels: `hostname` and `model`.
The second entity has a third label: `rack`.
Labels are user-defined and can vary between entities.
Only one label is special and required: the ID label.

Since servers are typically identified by hostname, the `hostname` label is the ID label.
(ID can be more than one label.)
Uniqueness is enforced by a unique key on the backend data store; Etre currently support MongoDB and MongoDB-compatible data stores.

## Data Modeling

Data modeling in Etre is denormalized by entity type.

This is more difficult than it sounds and more important than it seems.
To understand why&mdash;and to learn how to model data properly in Etre&mdash;it's imperative to understand the fallacy of false structure.

### False Structure

_False structure_ is a subjective view of resource organization that purports to be objective.

When engineers insist "_This_ is how the resources are organized," it's usually a false structure.
"This" is one way the resources can be organized, but there are probably many different ways depending on one's point of view.

Consider a typical setup of racks, servers, and databases:<br><br>

<img alt="Rock-Host-Db Tree" src="img/rack-host-db-tree.svg" style="width:480px"><br>

This hierarchical structure is not wrong, but there are at least three points of view:

* Data center engineer cares about racks and hosts
* DBA cares about databases and maybe hosts
* Application developer care only about their databases

Depending on one's point of view, the structure changes.
Application developers, for example, often see the resources structured this way:

```
app/
  env/
    region/
      db-cluster/
        db-node/
```

Also notice that the diagram and the directory tree capture different aspects of the resources.
In the diagram, app, env, and region aren't visible.
In the directory tree, these three aspects are clearly visible but hosts are not.

Realizing and accepting that <u>the same resources can have different views</u> is key to understanding Etre and proper Etre data modeling.

Any structure is valid if helps a person (or machine) find what they need and accomplish what they're trying to do.
However, that presents a challenge: how do you model "any structure"?
Answer: you don't; you model what exists and let a user's point of view generate a (virtual) structure:

`What Exists + Point of View = Structure`

### What Exists

Etre data modeling starts by identify _what exists_.

Start with obvious, uncontentious resources:
* A physical server (host) obviously exist
* A network switch obviously exists
* A database instance (node) obviously exists
* A service (app) obviously exists

When it comes to less obvious resources, like a database cluster, follow these principles:

* **Entity Principle**: If removing it leaves nothing, it's an entity.
(Or, other way around: if it's removed and something remains, it's not an entity; what remains is the entity.)
<br><br>
* **Indivisible Principle**: An entity doesn't depend on parts to exists. Without getting into a [Ship of Theseus paradox](https://en.wikipedia.org/wiki/Ship_of_Theseus), a server is still a server when a hard drive is removed.
<br><br>
* **ID Principle**: An entity must be uniquely identifiable within its type.
If an ID isn't obvious, then it might not be an entity.
<br><br>
* **Scalar Principle**: Labels and label values should be scalar (single value), not lists or enumerated.
(Enumerated means label1, label2, ..., labelN.)
If non-scalar labels or values are needed, it's inverted&mdash;denormalize it.
<br><br>
* **Sparse Principle**: Entities should _not_ be sparse.
If an entity has very few labels, it's probably not an entity.
The labels probably belong to another entity type.
<br><br>
* **Stability Principle**: Labels and values should be stable and long-lived.
(Write once, read many is ideal.)
If not, it's probably a resource (or label) that should not be stored in Etre.
<br><br>
* **Binary Principle**: Querying more than 2 entity types is usually an anti-pattern and a sign of [false structure](#false-structure).
<br><br>
* **Duplication Principle**: Judicious duplication is necessary and normal.
Trying to avoid duplication usually leads to [false structure](#false-structure).
Duplication also helps avoid violating the Binary Principle.
<br><br>
* **Pragmatic Principle**: Whatever makes real-world usage fast and easy is acceptable.
Use this principle sparingly and only as a last resort because it tends to create short-tem solutions and long-term problems.
<br><br>

Is a database cluster an entity?
This can be argued both ways depending on the type of cluster:

_No_
* A traditional database "cluster" (i.e. replication topology) is not an entity because if you take away the cluster the nodes remain&mdash;the Entity and Indivisible principles do not hold.
It's also likely that the Sparse Principle doesn't hold either because, given the Duplication Principle, any cluster-level settings can and should be duplicated into the node entities.
Instead, the database node entities should have a cluster label; and when a user (or program) wants to "build" the cluster, the query includes cluster=value.

_Yes_
* Sometimes clusters can be "headless": have no database instances.
In this case, the Entity and Indivisible principles hold.
The Sparse Principle might still be violated, but then the Pragmatic Principle applies: headless clusters are unusual but they do exist for some purposes.
In this case, the Scalar and Binary principles are important to avoid introducing false structures.
Even with a cluster entity type, labels like cluster name should be put (Duplication Principle) on database instance entities, too.

### Labels (Point of View)

`What Exists + Point of View = Structure`

Labels allow many points of view on the same underlying resources.
When querying Etre, users select labels to match _and_ labels to return.
An important principle and design of Etre is that the usage of labels cannot be known ahead of time.

Granted, users can and should know most common access patterns.
But experience proves that there's always some new combination of labels applied to (or projected from) the resources.

All labels should actual, useful, and consistent:

* **Actual**: The label and value are actual properties of the resource.
If a label is being put on an entity because it's unclear where else it should go, it probably indicates another entity type.
No entity is a dumping ground for labels.
* **Useful**: The label is either queried or returned (projected).
If a label is never used, it shouldn't be stored. This helps guard against [what not to store](#what-not-to-store).
* **Consistent**: Labels should be consistent across all entities and entity types. See [Conventions](#conventions).

The latter two are the usual problems: storing useless labels and not following (or violating) a convention.

A similar "pragmatic principle" applies.
Storing, for a example, a _slowly_ changing status value as a label is probably acceptable, especially if it eventually ends on a steady state.

## Conventions

* Singular noun entity names: host, switch, app
* Snake_case names and labels: aws_region, db_id, backup_retention
* Consistent values: "prod" or "production", not both
* Terse but not cryptic: db_id _not_ database_identifier [1]
* Lowercase names and labels

[1] Terse is important for a denormalized data model with high duplication. At scale, every byte matters.

## What _Not_ to Store

* Ephemeral data, values, properties (status, roles, etc.)
* Non-atomic values: lists, document/objects, CSV
* Enumerated labels: `"foo1": "...", "foo2": "..."`
* User-created features: indexes, feature flags, etc.

## Data Format

Etre is pseudo-schemaless and format-flexible.
Internally, every entity is stored like:

```json
{
    "_id": "b93aksdfjz09",
    "_type": "host",
    "_rev": 1,
    "key1": "val1",
    "keyN": "valN"
}
```

`_id`, `_type` and `_rev` are internal fields that the user cannot change.
`_id` is the data store ID (not the user-level Etre ID label).
`_type` is the Etre entity type.
`_rev` is the revision of the entity, incremented on every write.
The latter two fields are necessary for the change data capture (CDC) stream.

Entities are stored as JSON objects on the back end (in the data store), but they are also represented as key-value CSV _text_ (strings) by the CLI:

```
_id:b93aksdfjz09,_type:host,_rev:1,key1:val1,keyN:valN
```

The CLI defaults to key-value CSV text because it's intended to be processed with Bash and command-line tools like `cut`, `sed`, and `awk`.
