import { styled } from "@operational/components";
import React from "react";
import { DroppableStateSnapshot } from "react-beautiful-dnd";

const DropZoneContainer = styled.div<DroppableStateSnapshot & { color: string }>`
  min-height: 20px;
  background-color: ${props => props.color};
  border-radius: 2px;
  border: dashed 1px ${props => props.theme.color.border.default};
  display: flex;
  align-items: center;
  padding: 0 8px;
  color: ${props => props.theme.color.text.lighter};
  opacity: ${props => (props.isDraggingOver ? 0 : 1)};
  font-family: ${props => props.theme.font.family.main};
  font-weight: ${props => props.theme.font.weight.medium};
  transition: opacity 0.3s;
`;

const measureColor = "rgba(60, 195, 70, 0.1)";
const dimensionColor = "rgba(20, 153, 206, 0.1)";

export const MeasureDropZone: React.SFC<DroppableStateSnapshot> = props => (
  <DropZoneContainer {...props} color={measureColor}>
    drag measures here…
  </DropZoneContainer>
);

export const DimensionDropZone: React.SFC<DroppableStateSnapshot> = props => (
  <DropZoneContainer {...props} color={dimensionColor}>
    drag dimensions here…
  </DropZoneContainer>
);

export const OrderDropZone: React.SFC<DroppableStateSnapshot> = props => (
  <DropZoneContainer {...props} color={dimensionColor}>
    drag rows or measures here…
  </DropZoneContainer>
);
