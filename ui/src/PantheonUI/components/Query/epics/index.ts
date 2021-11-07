import { combineEpics } from "redux-observable";

import aggregateEpics from "./aggregateEpics";
import recordEpics from "./recordEpics";
import sqlEpics from "./sqlEpics";


export default combineEpics(aggregateEpics, recordEpics, sqlEpics);
