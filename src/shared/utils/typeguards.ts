export const hasStringProperty = <PropName extends string, Input extends Record<string, unknown> | null>(
  propName: PropName,
  v: Input,
): v is Input & { [k in PropName]: string } =>
  typeof v === "object"
  && v !== null
  && propName in v
  && typeof v[propName] === "string";

export const nameOf = <Shape>(key: keyof Shape) => key;
