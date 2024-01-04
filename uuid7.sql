/*
 * UUID7.sql
 * 
 * A Proof-of-Concept for a PL/PGSQL internal implementation of UUID v7
 * that could be useful until direct internal or extension PostgreSQL 
 * supports UUID version 7.
 */


create extension if not exists pgcrypto;


/*
 * Get epoch time as milliseconds
 * This is a direct PL/PGSQL implementation of the `get_milliseconds` function
 * from the UUID7 C language code found here:
 * https://gist.github.com/fabiolimace/9873fe7bbcb1e6dc40638a4f98676d72
 */
create or replace function public.get_milliseconds(ts timestamptz default null::timestamptz)
returns bigint
as $$
declare
    uuid_ts timestamptz;
    ms bigint;
begin
    uuid_ts = coalesce(ts, clock_timestamp());
    return (
        (date_part('epoch', uuid_ts)::bigint * 1000::bigint)::bigint +
        (
            (
                date_part('seconds', uuid_ts::timestamptz) - 
                date_part('seconds', uuid_ts::timestamptz)::bigint
            ) * 
            1000::bigint
        )::bigint
    )::bigint;
end; $$
language plpgsql;


/*
 * Use pgcrypto to generate num_bytes of random byte data.
 * Convert each byte to an int representation and return
 * int[].
 * This is a direct PL/PGSQL implementation of the `get_random_bytes` function
 * from the UUID7 C language code found here:
 * https://gist.github.com/fabiolimace/9873fe7bbcb1e6dc40638a4f98676d72
 */
create or replace function public.get_random_bytes(num_bytes int)
returns int[]
as $$
declare
    rbytes bytea;
    intarr int[];
    i int = 0;
begin
    if (num_bytes < 1) or (num_bytes > 1024)
    then
        raise exception 'The number of bytes to generate must be between 1 and 1024 inclusive';
    end if;

    intarr = array_fill(0::int, ARRAY[num_bytes]);
    rbytes = gen_random_bytes(num_bytes);
    while i < num_bytes
    loop
        intarr[i + 1] = get_byte(rbytes, i);
        i = i + 1;
    end loop;

    return intarr;
end; $$
language plpgsql
returns null on null input;


/*
 * Build a uuid hex digest from a 16-element int array
 */
create or replace function public.uuid_raw_to_hex(uuid_raw int[])
returns text
as $$
declare
    UUID_T_LEN int = 16;
    SEP_INDEXES int[] = ARRAY [
        5::int, 8, 11, 14
    ];
    i int = 1;
    j int = 1;
    uuid_digits text[] = ARRAY [
        null::text, null, null, null, 
        '-',
        null, null, 
        '-',
        null, null,
        '-',
        null, null, 
        '-',
        null, null, null, null, null, null
    ];
begin
    while i <= UUID_T_LEN
    loop
        if i in (5, 8, 11, 14)
        then
            j = j + 1;
        end if;
        
        uuid_digits[j] = lpad(to_hex(uuid_raw[i] & 0xff), 2, '0');
        
        i = i + 1;
        j = j + 1;
    end loop;

    return array_to_string(uuid_digits, '');
end; $$
language plpgsql
returns null on null input;


/* 
 * Create a stateless UUID7 and return as PG uuid type
 * This is a direct PL/PGSQL implementation of the `create_uuid7_stateless` function
 * from the UUID7 C language code found here:
 * https://gist.github.com/fabiolimace/9873fe7bbcb1e6dc40638a4f98676d72
 */
create or replace function public.gen_stateless_uuid7(uuid_ts timestamptz default null::timestamptz)
returns uuid
as $$
declare
    -- UUID_T_LEN int = 16;
    UUID_TS_LEN int = 6;
    UUID7_VER_BYTE int = 6;
    UUID7_VER_OR int = 0x0f;
    UUID7_VAR_BYTE int = 8;
    UUID7_VAR_OR int = 0x3f;
    -- RAND_A_LEN int = 2;
    -- RAND_B_LEN int = 8;
    RAND_AB_LEN int = 10;
    i int = 1;
    uuid_ms bigint = 0::bigint;
    rand_ab int[] = ARRAY [
        0::int, 0, 0, 0, 0,
        0, 0, 0, 0, 0
    ];
    uuid7_raw int[] = ARRAY [
        0::int, 0, 0, 0, 0, 0, 0, 0, 
        0, 0, 0, 0, 0, 0, 0, 0
    ];
begin
    uuid_ms = public.get_milliseconds(uuid_ts);
    while i <= UUID_TS_LEN
    loop
        uuid7_raw[UUID_TS_LEN - (i + 1)] = (uuid_ms >> (8 * (i - 1))) & 0xff;
        i = i + 1;
    end loop;

    rand_ab = public.get_random_bytes(RAND_AB_LEN);
    i = 1;
    while i <= RAND_AB_LEN
    loop
        uuid7_raw[UUID_TS_LEN + i] = rand_ab[i];
        i = i + 1;
    end loop;

    uuid7_raw[UUID7_VER_BYTE] = 0x70 | (uuid7_raw[UUID7_VER_BYTE] & UUID7_VER_OR);
    uuid7_raw[UUID7_VAR_BYTE] = 0x80 | (uuid7_raw[UUID7_VAR_BYTE] & UUID7_VAR_OR);

    return public.uuid_raw_to_hex(uuid7_raw)::uuid;    
end $$
language plpgsql;


/*
 * Given this implementation, I'm not sure I can get stateful generation working, 
 * at least not for a PoC.
 */


/*
 * For future stateful implementations
create table if not exists public.uuid7_states (
    uuid_ts bigint not null,
    rand_a int[2],
    rand_b int[8]
)
;
 */

/* Various stateful implementations to follow. */

