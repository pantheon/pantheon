import { styled } from "@operational/components";
import { kebab } from "case";
import React from "react";

export interface ProductBoxProps {
  onClick?: () => void;
  imageUrl?: string;
  label: string;
}

const imageSize = 48;

const ImageContainer = styled("div")`
  width: ${imageSize}px;
  margin-right: ${({ theme }) => theme.space.content}px;
`;

const Container = styled("div")`
  box-shadow: none;
  border: 1px solid ${props => props.theme.color.border.disabled};
  height: 80px;
  margin: 0;
  padding: ${props => props.theme.space.element}px;
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: flex-start;

  :hover {
    background: ${props => props.theme.color.background.lighter};
  }

  img {
    width: 100%;
    margin: 0;
  }
`;

const Label = styled("p")`
  font-size: ${props => props.theme.font.size.body};
  font-weight: ${props => props.theme.font.weight.bold};
  margin: 0;
  color: ${props => props.theme.color.text.light};
`;

const ProductBox: React.SFC<ProductBoxProps> = props => (
  <Container data-cy={`pantheon--data-source-wizard--source__${kebab(props.label)}`} onClick={props.onClick}>
    <ImageContainer>{<img src={props.imageUrl} />}</ImageContainer>
    <Label>{props.label}</Label>
  </Container>
);

export default ProductBox;
