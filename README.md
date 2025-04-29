# Welcome to FaunaDB

FaunaDB is an operational database that combines document flexibility with native relational capabilities, strong consistency, global distribution, ACID transactions, and Internet-native HTTPS connectivity.

# Getting started with development

FaunaDB runs on the JVM and requires a JDK version 17 installed. Fauna uses [sbt](https://www.scala-sbt.org/download/) as its build tool.

FaunaDB is broken out into multiple subprojects primarily found in `ext/`. See the top level [`build.sbt`](build.sbt) file to get oriented on project structure. However the top level project aggregates a number of standard sbt commands

## Basics

Below are a few FaunaDB-specific sbt examples. See sbt's documention or inspect FaunaDB's sbt configuration to learn more.

**Compile all code and tests:**

```bash
$ sbt Test/compile
```

**Run all tests (warning, this will take a while):**

```bash
$ sbt test
```

NOTE: Some tests (specifically those in the `multicore` subproject require that multiple loopback IP addresses are available. On macOS, you can configure these with the following command (which last until the next restart):

```bash
$ for i in {2..16}; do sudo ifconfig lo0 alias 127.0.0.$i up; done
```

**Run tests in a specific subproject (e.g. `model`):**

```bash
$ sbt model/test
```

**Run test Specs which match a specific glob:**

```bash
$ sbt model/testOnly *DatabaseSpec
```

**Run a temporary database node for development:**

(Warning, this will reset persisted state every time!)

```bash
$ sbt runCore
```

## Building a release

FaunaDB is built as a fat JAR for release purposes, which includes all dependencies in one JAR file. The provisional script `mktarball.sh` will use sbt to build a release JAR and create a tarball with supporting scripts. Once you have built this JAR, see [OPERATING.md](OPERATING.md) for instructions on how to deploy and operate the service.

# Key concepts and features

## Relational model

Fauna’s data model integrates the best aspects of document and relational databases. Like other document databases, data is stored in JSON-like documents, allowing for the storage of unstructured data and removing the need for an object-relational mapper (ORM) to translate between objects and tables. Fauna also provides key features of relational databases including strong consistency, first-class support for relationships between documents, and the ability to layer on and enforce schema over time.

Documents in Fauna are organized into collections, similar to tables in relational databases, providing a familiar structure for data organization. Collections can specify document types, which define and enforce the structure of documents they contain. This feature allows developers to start with a flexible schema and gradually introduce more structure as their application matures. Importantly, Fauna supports relationships between documents in different collections, enabling complex data modeling without duplicating data. This approach combines the ease of use of document databases with the powerful data modeling capabilities of relational systems.

## Fauna Query Language

Fauna Query Language (FQL) is a TypeScript-inspired language designed specifically for querying and manipulating data in Fauna. It offers a concise, yet expressive syntax for relational queries, supporting complex joins and data transformations. FQL includes optional static typing to catch errors early in development, improving code quality and reducing runtime issues.

One of FQL’s powerful features is the ability to create user-defined functions (UDFs). These allow developers to encapsulate complex business logic directly within the database, promoting code reuse and maintaining a clear separation of concerns.

Here’s an example of an FQL query:

``` fql
// Gets the first customer with
// an email of "alice.appleseed@example.com".
let customer = Customer.where(.email == "alice.appleseed@example.com")
                .first()

// Gets the first order for the customer,
// sorted by descending creation time.
Order.where(.customer == customer)
  .order(desc(.createdAt)).
  first() {
    // Project fields from the order.
    // The order contains fields with document references.
    // Projecting the fields resolves references,
    // similar to a SQL join.
    // `Customer` document reference:
    customer {
      name,
      email
    },
    status,
    createdAt,
    items {
      // Nested `Product` document reference:
      product {
        name,
        price,
        stock,
        // `Category` document reference:
        category {
          name
        }
      },
      quantity
    },
    total
  }
```

The query shows how FQL can succinctly express complex operations, including lookups, joins, sorting, and data projection.

## Fauna Schema Language

Fauna Schema Language (FSL) allows developers to define and manage database schema as code. It enables version control for schema changes, integration with CI/CD pipelines, and progressive schema enforcement as applications evolve. By treating database schema as code, teams can apply the same rigorous review and testing processes to database changes as they do to application code.

Here’s an example of an FSL schema definition:

``` fsl
collection Customer {
  name: String
  email: String
  address: {
    street: String
    city: String
    state: String
    postalCode: String
    country: String
  }

  compute cart: Order? = (customer => Order.byCustomerAndStatus(customer, 'cart').first())

  // Use a computed field to get the Set of Orders for a customer.
  compute orders: Set<Order> = ( customer => Order.byCustomer(customer))

  // Use a unique constraint to ensure no two customers have the same email.
  unique [.email]

  index byEmail {
    terms [.email]
  }
}
```

This schema defines a Customer collection with specific fields, a computed field, a uniqueness constraint, and an index. The *: Any wildcard constraint allows for arbitrary ad hoc fields, providing flexibility while still enforcing structure where needed.

## Transactions and consistency

In Fauna, every query is a transaction, ensuring ACID compliance across all operations, even in globally distributed region groups. Fauna’s distributed transaction engine, based on the Calvin protocol, provides strict serializability for all read-write queries and serializable isolation for read-only queries.

## Security and access control

Fauna provides comprehensive security features to protect your data and control access. It offers role-based access control (RBAC) for coarse-grained permissions and attribute-based access control (ABAC) for fine-grained, dynamic access rules. This combination allows for highly flexible and precise control over who can access and modify data.

The system includes built-in user authentication and also supports integration with third-party identity providers, allowing you to use existing authentication systems.

## Change data capture (CDC) and real-time events

Fauna’s Change data capture (CDC) feature enables real-time application features. Developers can subscribe to changes in collections or specific documents, receiving either atomic push updates using real-time event streams or on-demand batch pull updates using event feeds. Events are particularly useful for maintaining live application states, building collaborative features that require real-time synchronization, and mirroring changes into external systems.

## Database model

Fauna’s database model makes it easy to create databases for isolated environments, such as staging and production, and build secure multi-tenant applications.

For multi-tenant applications, developers can create isolated child databases for each tenant, applying separate access controls and schema as needed.

The same approach can be applied to isolated environments, enabling separate databases for each environment, ensuring that changes in one environment do not affect others.

This model simplifies administration, ensures clear separation between environments, and guarantees that tenants or environments cannot interfere with each other’s data.

# License

Fauna is available under license. See LICENSE.md for a copy of the license.
