import { isInteger, isRecord, isString } from 'typechecked';
import RoachKV from '../main.ts'

const tableDefinition = {
    users: {
        fields: {
            firstName: isString,
            lastName: isString,
            age: isInteger,
            address: {
                line1: isString,
            }
        }
    }
}

const kv = RoachKV("postgres://root@localhost:26257/roachkv", tableDefinition)

async function main() {
    await kv.delete(['users', 'newuser']);

    await kv.set(['users', 'newuser'], {
        firstName: "Alloys",
        lastName: "Mila",
        age: 23
    })

    let data = await kv.get(['users', 'newuser'])
    console.log("new data");
    console.log(data);

    console.log("updated step")
    let update = await kv.update(['users', 'newuser'], { age: 13 })

    console.log("updated data");

    let newdata = await kv.get(['users', 'newuser'])
    console.log(newdata.data.firstName);
}

main();
