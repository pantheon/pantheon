import { styled } from "@operational/components";
import React from "react";
import { FilterTreeOperatorNode, isUnmatchedParenthesis } from "./parseFilter";

export interface OperatorProps
  extends Pick<FilterTreeOperatorNode, "closeParentheses" | "operator" | "openParentheses"> {
  onCloseParenthesisAdd: () => void;
  onCloseParenthesisRemove: (parenthesisId: number) => void;
  onOpenParenthesisAdd: () => void;
  onOpenParenthesisRemove: (parenthesisId: number) => void;
  onOperatorToggle: () => void;
  disabled: boolean;
  /**
   * Top position, every element are absolute to solve the interaction mix between these elements
   * and drag & drop.
   */
  top: number;
  canOpenParenthesis: boolean;
  canCloseParenthesis: boolean;
}

export const Operator: React.SFC<OperatorProps> = props => (
  <Container top={props.top}>
    {!props.disabled && (
      <>
        <Area>
          {props.closeParentheses.map(id => (
            <Item
              left
              key={id}
              invalid={isUnmatchedParenthesis(id)}
              onClick={() => props.onCloseParenthesisRemove(id)}
            >{`)`}</Item>
          ))}
          {props.canCloseParenthesis && (
            <Item className="action-button" left ghost onClick={props.onCloseParenthesisAdd}>{`)`}</Item>
          )}
        </Area>
        {props.operator ? (
          <Item center onClick={props.onOperatorToggle}>
            {props.operator}
          </Item>
        ) : (
          <div />
        )}
        <Area right>
          {props.canOpenParenthesis && (
            <Item className="action-button" right ghost onClick={props.onOpenParenthesisAdd}>{`(`}</Item>
          )}
          {props.openParentheses.map(id => (
            <Item
              right
              key={id}
              invalid={isUnmatchedParenthesis(id)}
              onClick={() => props.onOpenParenthesisRemove(id)}
            >{`(`}</Item>
          ))}
        </Area>
      </>
    )}
  </Container>
);

const Container = styled.div<{ top: number }>`
  height: 5px;
  position: absolute;
  width: 100%;
  top: ${props => props.top}px;
  border-top: 1px dashed ${props => props.theme.color.border.default};
  display: grid;
  grid-template-columns: 1fr min-content 1fr;
  margin-top: ${props => props.theme.space.element / 2}px;

  .action-button {
    opacity: 0;
  }

  :hover .action-button {
    opacity: 1;
  }
`;

const Area = styled.div<{ right?: boolean }>`
  display: flex;
  ${props => (props.right ? "justify-content: flex-end;" : "")}
`;

const Item = styled.div<{ center?: boolean; left?: boolean; right?: boolean; ghost?: boolean; invalid?: boolean }>`
  position: relative;
  cursor: pointer;
  color: ${props => props.theme.color.text.default};
  top: -${props => props.theme.space.element / 2}px;
  height: ${props => props.theme.space.element}px;
  width: ${props => (props.center ? "32px" : "24px")};
  text-align: center;
  border-radius: ${props => props.theme.borderRadius}px;
  border: 1px
    ${props =>
      props.ghost ? `dashed ${props.theme.color.border.disabled}` : `solid ${props.theme.color.border.default}`};
  background-color: ${props => props.theme.color.white};
  ${props => (props.left ? "margin-right: 4px" : props.right ? "margin-left: 4px" : "")};
  font-family: ${props => props.theme.font.family.main};
  font-size: ${props => props.theme.font.size.small}px;
  font-weight: ${props => props.theme.font.weight.regular};
  ${props => (props.invalid ? "border: 1px solid red;" : "")}

  :hover {
    color: ${props => props.theme.color.primary};
    background-color: ${props => props.theme.color.background.lightest};
  }
`;
