import { Card, CardProps, styled } from "@operational/components";

interface Props {
  fullHeight?: boolean;
}

const NakedCard = styled(Card)`
  background-color: transparent;
  box-shadow: none;
  border: 0;

  ${(props: Props) =>
    props.fullHeight
      ? `
    > div:nth-child(2) {
      max-height: calc(100vh - 170px);
      overflow: auto;
    }
  `
      : ""}
` as <T extends {}>(props: CardProps<T> & Props) => JSX.Element;

export default NakedCard;
