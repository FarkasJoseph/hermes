import Long from 'long';

import type * as Rpc from '@gov.nasa.jpl.hermes/rpc';
import type { CommandValue } from '@gov.nasa.jpl.hermes/sequence';
import { Def, Dictionary, DualKeyMap, Proto, Value } from '@gov.nasa.jpl.hermes/types';

/**
 * A Hermes dictionary of instance's commands. Each command is keyed by its YAMCS
 * qualified name, like /BigData_YamcsDeployment/CdhCore/cmdDisp/CMD_NO_OP_STRING,
 * and keeps that name in metadata.qualifiedName.
 *
 * A YAMCS command inherits the arguments of the commands it builds on, and any
 * command in that chain can assign an argument a value, as fprime-yamcs does for
 * OpCode. We leave the assigned arguments out of the command, and keep their
 * values in metadata.assignments.
 */
export function yamcsDictionary(instance: string, commands: readonly Rpc.YamcsCommandInfo[]): Proto.IDictionary {
    const dictionary = new Dictionary({ name: instance, type: 'yamcs' });
    const namespace = dictionary.get('');
    for (const info of commands) {
        if (info.qualifiedName && info.name) {
            namespace.addCommand(info.qualifiedName, yamcsCommand(info));
        }
    }
    return { ...dictionary.toProto(), id: `yamcs:${instance}` };
}

function yamcsCommand(info: Rpc.YamcsCommandInfo): Def.Command {
    const qualifiedName = info.qualifiedName!;
    // YAMCS lists inherited arguments before a command's own, so we add each base command to the front of chain
    const chain: Rpc.YamcsCommandInfo[] = [];
    for (let c: Rpc.YamcsCommandInfo | undefined = info; c; c = c.baseCommand) {
        chain.unshift(c);
    }
    const assignments = Object.fromEntries(chain.flatMap((c) => c.argumentAssignment ?? []).map((a) => [a.name, a.value]));
    const command: Def.Command = {
        mnemonic: info.name!,
        component: qualifiedName.substring(0, qualifiedName.lastIndexOf('/')),
        arguments: [],
        metadata: { description: info.shortDescription || info.longDescription, qualifiedName, assignments },
    };

    try {
        command.arguments = chain.flatMap((c) => c.argument ?? [])
            .filter((arg) => !Object.hasOwn(assignments, arg.name!))
            .map((arg) => ({ name: arg.name!, type: argumentType(arg.type!), metadata: { description: arg.description } }));
    } catch (err) {
        // We keep a command whose argument types we can't convert, and record why
        command.metadata!.error = (err as Error).message;
    }
    return command;
}

/**
 * The Hermes type for a YAMCS argument type. engType names the kind of type, such
 * as integer or float. An array's engType ends in a single [], even for an array
 * of arrays, so we read the element's type from elementType.
 */
function argumentType(type: Rpc.YamcsArgumentTypeInfo): Def.Type {
    if (type.engType?.endsWith('[]')) {
        // We need one fixed-size dimension. YAMCS can't issue array arguments with more, and a Hermes array can't
        // take its size from another argument or a parameter.
        const [dimension, ...more] = type.dimensions ?? [];
        if (dimension?.fixedValue === undefined || more.length > 0) {
            throw new Error(`${type.name} isn't a fixed-size array with one dimension, which isn't supported`);
        }
        return { kind: Def.TypeKind.array, name: type.name, type: argumentType(type.elementType!), size: Number(dimension.fixedValue) };
    }

    switch (type.engType) {
        case 'integer':
            return integerType(type);
        case 'float':
            return { kind: type.dataEncoding?.sizeInBits === 32 ? Def.TypeKind.f32 : Def.TypeKind.f64 };
        case 'string':
            return { kind: Def.TypeKind.string, maxLength: type.maxChars };
        case 'boolean':
            return { kind: Def.TypeKind.boolean };
        case 'enumeration':
            return {
                kind: Def.TypeKind.enum,
                name: type.name ?? '',
                values: new DualKeyMap('value', (type.enumValue ?? []).map((e) => [e.label!, {
                    name: e.label!,
                    value: Number(e.value),
                    metadata: { description: e.description },
                } satisfies Def.EnumItem])),
            };
        case 'aggregate':
            return {
                kind: Def.TypeKind.object,
                name: type.name,
                fields: (type.member ?? []).map((m) => ({
                    name: m.name!,
                    type: argumentType(m.type!),
                    metadata: { description: m.shortDescription || m.longDescription },
                })),
            };
        default:
            throw new Error(`${type.name} has engType ${type.engType}, which isn't supported`);
    }
}

/**
 * The smallest Hermes integer that holds a YAMCS integer type, which can have any
 * number of bits. When the type has fewer bits than that integer, or a rangeMin or
 * rangeMax, we set min and max to the range the type allows.
 */
function integerType(type: Rpc.YamcsArgumentTypeInfo): Def.NumberType {
    const bits = type.dataEncoding?.sizeInBits ?? 64;
    const signed = type.signed!;
    const width = [8, 16, 32, 64].find((w) => bits <= w) ?? 64;
    const kinds: Record<number, Def.NumberTypeKind> = signed
        ? { 8: Def.TypeKind.i8, 16: Def.TypeKind.i16, 32: Def.TypeKind.i32, 64: Def.TypeKind.i64 }
        : { 8: Def.TypeKind.u8, 16: Def.TypeKind.u16, 32: Def.TypeKind.u32, 64: Def.TypeKind.u64 };
    const out: Def.NumberType = { kind: kinds[width] };
    // On load, Dictionary.fromProto drops a range with only one end, so we fill a missing end from bits
    if (bits < width || type.rangeMin !== undefined || type.rangeMax !== undefined) {
        const lowest = signed ? -(2 ** (bits - 1)) : 0;
        const highest = signed ? 2 ** (bits - 1) - 1 : 2 ** bits - 1;
        out.min = Math.max(lowest, type.rangeMin ?? -Infinity);
        out.max = Math.min(highest, type.rangeMax ?? Infinity);
    }
    return out;
}

/**
 * The arguments of cmd as IssueCommand takes them, keyed by argument name
 */
export function yamcsArguments(cmd: CommandValue): Record<string, Rpc.YamcsArgumentValue> {
    return Object.fromEntries(cmd.def.arguments.map((arg, i) => [arg.name, argumentValue(cmd.args[i])]));
}

/**
 * Converts an argument value from a notebook cell into the protobuf Value that
 * YAMCS reads arguments from.
 */
export function argumentValue(value: Value): Rpc.YamcsArgumentValue {
    if (typeof value === 'string') {
        return { stringValue: value };
    } else if (typeof value === 'number') {
        return { numberValue: value };
    } else if (typeof value === 'boolean') {
        return { boolValue: value };
    } else if (Long.isLong(value) || typeof value === 'bigint') {
        // A protobuf Value holds numbers as doubles, which can't hold every 64-bit integer, so we send one
        // as text, which YAMCS parses. A bigint here is an element of a BigInt64Array or BigUint64Array.
        return { stringValue: value.toString() };
    } else if (Array.isArray(value) || ArrayBuffer.isView(value)) {
        return { listValue: { values: Array.from(value as ArrayLike<Value>, argumentValue) } };
    } else {
        return {
            structValue: {
                fields: Object.fromEntries(Object.entries(value).map(([name, member]) => [name, argumentValue(member)])),
            },
        };
    }
}

const failedStatuses = ['NOK', 'TIMEOUT', 'CANCELLED'];

/**
 * Whether YAMCS has sent the command in entry. YAMCS records each step of sending
 * a command in its history as <step>_Status, for example Acknowledge_Queued_Status,
 * and why a step failed as <step>_Message. We return true once Acknowledge_Sent_Status
 * is OK, false while YAMCS is still working on it, and throw YAMCS's reason if a step
 * failed first.
 *
 * We check for sent first, because YAMCS can record a failure status afterwards, such
 * as CANCELLED for a verifier it stops waiting on.
 */
export function commandSent(entry: Rpc.YamcsCommandHistoryEntry): boolean {
    const attributes = new Map((entry.attr ?? []).map((a) => [a.name, a.value?.stringValue]));
    if (attributes.get('Acknowledge_Sent_Status') === 'OK') {
        return true;
    }
    for (const [name, status] of attributes) {
        if (name?.endsWith('_Status') && status && failedStatuses.includes(status)) {
            const step = name.slice(0, -'_Status'.length);
            const reason = attributes.get(`${step}_Message`);
            throw new Error(`YAMCS did not send ${entry.commandName}: ${step} ${status}${reason ? `, ${reason}` : ''}`);
        }
    }
    return false;
}
