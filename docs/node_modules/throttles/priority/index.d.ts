type isDone = () => void;
type toAdd = (fn: Function, isHigh?: boolean) => void;
declare function throttles(limit?: number): [toAdd, isDone];
export default throttles;
