import { Icon, styled } from "@operational/components";
import TreeItem from "@operational/components/lib/Tree/TreeItem";
import React, { useCallback, useContext } from "react";
import { PslColor } from "../../utils";
import { QueryEditorContext } from "../QueryEditor";
import { AddFilterModal } from "../QueryEditor/AddFilterModal";
import { FilterTreePredicateNode } from "./parseFilter";

export interface PredicateProps {
  item: FilterTreePredicateNode;
  index: number;
}

export const Predicate: React.FC<PredicateProps> = ({ item, index }) => {
  const { dispatch, confirm } = useContext(QueryEditorContext);

  const openUpdatePredicateModal = useCallback(() => {
    confirm({
      title: `Configure filter for ${item.dimension}`,
      body: AddFilterModal,
      state: {
        dimension: item.dimension,
        type: item.operator,
        value: item.value || "",
      },
      onConfirm: state =>
        dispatch({
          type: "[aggregate] updateFilterPredicate",
          payload: {
            dimension: item.dimension,
            index,
            operator: state.type,
            value: state.value,
            type: "predicate",
          },
        }),
    });
  }, [confirm, item, dispatch]);

  const removePredicate = useCallback(
    (e: React.MouseEvent<HTMLDivElement, MouseEvent>) => {
      e.stopPropagation();
      dispatch({
        type: "[aggregate] removeFilterPredicate",
        payload: { index },
      });
    },
    [dispatch],
  );

  return (
    <Container>
      <HitZone onClick={openUpdatePredicateModal}>
        <TreeItem
          tag="D"
          color={PslColor.dimension}
          label={item.dimension}
          highlight={false}
          isOpen={false}
          hasChildren={false}
        />
        <Operator>{item.operator}</Operator> {item.value}
        <RemoveButton className="remove-button" onClick={removePredicate}>
          <Icon name="No" size={12} />
        </RemoveButton>
      </HitZone>
    </Container>
  );
};

const Container = styled.div`
  height: 77px;
  padding-top: 20px; /* We can't use margin due to react-beautiful-dnd requirement */
`;

const HitZone = styled.div`
  position: relative;
  padding: 8px;
  margin: 0 -13px;
  :hover {
    background-color: ${props => props.theme.color.background.lightest};
  }

  :hover .remove-button {
    opacity: 1;
  }
`;

const Operator = styled.b`
  margin-left: 26px;
`;

const RemoveButton = styled.div`
  position: absolute;
  top: calc(50% - 6px);
  right: 20px;

  opacity: 0;
  transition: opacity 0.2s linear;

  svg {
    /* Override Icon cursor */
    cursor: pointer !important;
  }
`;
