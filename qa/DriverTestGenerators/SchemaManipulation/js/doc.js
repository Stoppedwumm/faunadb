const { Chance } = require("chance");

const chance = new Chance();

let getDoc = (version) => {
  switch (version) {
    case 0:
      return {
        v1_string: "mystring_v0",
        v1_number: 0,
        v1_boolean: false,
        v1_twotypes: "created at v0"
      };
    case 1:
      return {
        v1_string: "mystring_v1",
        v1_number: chance.floating(),
        v1_boolean: true,
        v1_twotypes: chance.string()
      };
    case 2:
      return {
        v2_string: "mystring_v2",
        v2_int: chance.age(),
        v2_boolean: true,
        v2_twotypes: chance.integer()
      };
    case 3:
      return {
        v3_string: "mystring_v3",
        v3_number: chance.floating(),
        v3_boolean: false,
        v3_twotypes: chance.string()
      };
    case 4:
      return {
        v4_string: "mystring_v4",
        v4_number: chance.floating(),
        v4_boolean: true,
        v4_twotypes: chance.integer()
      };
    default:
      throw new Error(`Schema version ${version} not supported.`);
  }
};

module.exports = {
  getDoc
};
