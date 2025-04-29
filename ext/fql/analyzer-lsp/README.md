# FQLX Language Server

This is the language server for FQL. This uses the compiled `fql` module from
core, and some typescript boilerplate to create a language server.

To compile, build `analyzer` first. Then, cd into this directory, and run this:

```
yarn install
yarn build
```

`yarn install` will install all dependencies. `yarn build` will compile the core
`fql` module to javascript, export the standard library from core, and compile
it into the final javascript module.

Once complete, `yarn start` will start up the language server.
