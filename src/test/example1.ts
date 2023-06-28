import { isInteger, isRecord, isString } from 'typechecked';
import RoachKV from '../main.ts'

const tableDefinition = {
    users: {
        fields: {
            name: {
                firstName: isString,
                lastName: isString,
                age: isInteger,
            }
        }
    }
}

const kv = RoachKV("postgres://root@localhost:26257/roachkv", tableDefinition)

async function main() {
    await kv.delete(['users', 'newuser']);

    await kv.set(['users', 'newuser'], {
        username: "Alloys"
    })

    let data = await kv.get(['users', 'newuser'])
    console.log("new data");
    console.log(data);

    console.log("updated step")
    let update = await kv.update(['users', 'newuser'], { age: 13 })

    console.log("updated data");

    let newdata = await kv.get(['users', 'newuser'])
    console.log(newdata);
}

main();
