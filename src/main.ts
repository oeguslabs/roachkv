import { postgres } from "./postgres.ts"
import { isPartialRecord, isRecord } from 'typechecked';

interface DeleteOptions {
    soft: boolean
}

const DefaultDeleteOptions = {
    soft: true
}

async function createTable(name: string, sql: postgres.Sql) {

    let tx = sql.begin(async (sql) => {
        await sql`
            CREATE TABLE ${sql(name)} (
                id VARCHAR(255) PRIMARY KEY,
                key VARCHAR(255),
                    data jsonb,
                    creation_date TIMESTAMP default now(),
                    last_updated_date TIMESTAMP default now(),

                    CONSTRAINT unique_id_key UNIQUE (id, key)
                );
            `

        await sql.unsafe(`CREATE INDEX idx_data_gin ON ${name} USING gin(data);`)
        await sql.unsafe(`CREATE INDEX idx_key ON ${name} (key);`)
    })

    try {
        await tx;
    } catch (err) {
        console.log(err);
    }
}

// type TableDefinition<TN extends string = string> = Record<TN, {
//     fields: Record<string, isRecordInput>
// }>

type ValidatorFunc = (arg: unknown, msg?: string) => unknown
type ValidatorInput = Record<string, ValidatorFunc | {[key: string]: ValidatorFunc}>

type TableDefinition<X extends ValidatorInput = ValidatorInput> = {
    readonly [K: string]: {
        fields: X
    }
}
type TableDefinitionKnownTable<T extends TableDefinition> = keyof T
type TableDefinitionKnownTableFields<
    D extends TableDefinition,
    X extends TableDefinitionKnownTable<D>
> = D[X]['fields']


type InputValueTypeForSet<T extends TableDefinition,
    TX extends TableDefinitionKnownTable<T>,
    F extends TableDefinitionKnownTableFields<T, TX> = TableDefinitionKnownTableFields<T, TX>
> = {
        [K in keyof F]: F[K] extends ValidatorFunc ? ReturnType<F[K]> : never
    }

export function RoachKV<T extends TableDefinition>(DB_URL: string, tableDefinition: T) {
    type KnownTable = keyof T
    const sql = postgres(DB_URL, {
        debug: true
    });

    return {
        sql,
        /**
         * 
         * @param key - The Key to retrieve. Format is [tableName, UniqueID]
         * @param value - The Value associated with Key
         */
        // InputValueTypeForSet<T, KT>
        async set<KT extends KnownTable>(identifier: [KT, string, string?], value: Record<string, unknown>) {
            const tableName = identifier[0] as string;
            const tableID = identifier[1]

            try {

                let key: null | string = null;

                if (identifier.length === 3) {
                    key = identifier[2] || null;
                }

                const validator = isRecord(tableDefinition[tableName].fields);
                validator(value)

                const query = sql`
                    INSERT INTO ${sql(tableName)} (id, key, data, creation_date, last_updated_date) 
                    VALUES (${tableID}, ${key}, ${JSON.stringify(value)}, NOW(), NOW()) RETURNING id
                `;

                return await query;
            } catch (err) {
                let error = err as any;
                if (error.code === "42P01") {
                    createTable(tableName, sql)
                    throw new Error("Something went wrong, please try again");
                } else {
                    throw err;
                }
            }
        },

        async update(key: [KnownTable, string], newData: Record<string, any>) {
            const [tableName, tableID] = key;

            const validator = isPartialRecord(tableDefinition[tableName].fields);
            validator(newData)

            const existing = await this.get(key);

            if (existing) {
                const updatedData = Object.assign({}, existing.data, newData)

                const query = await sql`
                    UPDATE ${sql(tableName as string)}
                    SET data = ${JSON.stringify(updatedData)}, last_updated_date = NOW()
                    WHERE id = ${tableID}
                    -- RETURNING id
                `;

                return query;
            } else {
                throw new Error("The provided key does not resolve to a record")
            }
        },

        async get(key: [KnownTable, string, string?]) {

            const tableName = key[0]
            const tableID = key[1]
            const tableKey = key[2]

            let query;

            if (key.length === 2) {
                query = await sql`
                  SELECT * FROM ${sql(tableName as string)} WHERE id = ${tableID}
                `;
            } else {
                query = await sql`
                  SELECT * FROM ${sql(tableName as string)} WHERE id = ${tableID} AND key = ${tableKey || null}
                `;
            }

            let value = query[0]
            if (value?.data) {
                value.data = JSON.parse(value.data)
            }

            type T = typeof tableDefinition[typeof tableName]['fields']
            type K = { [K in keyof T]: ReturnType<T[K]> }

            return value as {
                [K: string]: any
                data: K
            }
        },

        async delete(key: [KnownTable, string], _options: DeleteOptions = DefaultDeleteOptions) {
            const [tableName, tableID] = key;

            const query = await sql`
              DELETE FROM ${sql(tableName as string)} WHERE id = ${tableID}
            `;

            return query;
        }
    }
}

export default RoachKV
