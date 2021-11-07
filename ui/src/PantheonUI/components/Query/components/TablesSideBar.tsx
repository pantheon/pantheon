/**
 * This component has some intentional code repetition with other `Query` subcomponents, to be removed
 * once the design and code structure matures. This makes sure that different parts of `Query` can evolve
 * quickly and independently, and to avoid jumping on an uneducated abstraction prematurely.
 */
import { Tree } from "@operational/components";

import React from "react";

import { Tree as ITree } from "@operational/components/lib/Tree/Tree";
import { PslColor } from "../../../utils";
import { TableResponse } from "../types";

export interface DragHandlers {
  onTableDragStart: (_: TableResponse) => void;
  onTableDragEnd: (_: TableResponse) => void;
  onColumnDragStart: (_: string) => void;
  onColumnDragEnd: (_: string) => void;
}

export interface Props {
  tables: TableResponse[];
  dragHandlers: DragHandlers;
}

export class TablesSideBar extends React.Component<Props, {}> {
  private makeTablesTree = (table: TableResponse): ITree => {
    return {
      label: table.name,
      initiallyOpen: false,
      childNodes: [
        {
          tag: "T",
          color: PslColor.table,
          label: table.name,
          draggable: true,
          initiallyOpen: false,
          childNodes: [],
          onDragStart: () => this.props.dragHandlers.onTableDragStart(table),
          onDragEnd: () => this.props.dragHandlers.onTableDragEnd(table),
        },
        ...table.columns.map(columnName => ({
          label: columnName,
          tag: "C",
          color: PslColor.column,
          draggable: true,
          initiallyOpen: false,
          childNodes: [],
          onDragStart: () => this.props.dragHandlers.onColumnDragStart(columnName),
          onDragEnd: () => this.props.dragHandlers.onColumnDragEnd(columnName),
        })),
      ],
    };
  };

  public shouldComponentUpdate(nextProps: Readonly<Props>) {
    return (
      JSON.stringify(this.props.tables) !== JSON.stringify(nextProps.tables) ||
      this.props.dragHandlers !== nextProps.dragHandlers
    );
  }

  public render() {
    return <Tree trees={this.props.tables.map(this.makeTablesTree)} />;
  }
}
