import { useCallback, useState } from "react";

/**
 * Hook to toggle a boolean value.
 *
 * Example:
 * ```ts
 * const MyComponent = () => {
 *  const [light, toggleLight] = useToggle(true);
 *
 *  return (<>
 *    <div>light is {light ? "on" : "off"}</div>
 *    <button onClick={toggleLight}>on/off</button>
 *  </>)
 * }
 * ```
 *
 * @param initialValue
 */
export const useToggle = (initialValue: boolean): [boolean, () => void] => {
  const [value, setValue] = useState(initialValue);
  const toggle = useCallback(() => setValue(prev => !prev), []);
  return [value, toggle];
};
