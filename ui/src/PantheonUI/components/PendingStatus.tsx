import { OperationalStyleConstants, Small, Spinner, styled } from "@operational/components";
import formatDistance from "date-fns/formatDistance";
import React from "react";

export interface PendingStatusProps {
  startedAt: number;
}

export interface State {
  time: number;
}

const Container = styled("div")<{ theme?: OperationalStyleConstants }>`
  height: 36px;
  max-width: 160px;
  display: inline-flex;
  align-items: center;
  color: ${({ theme }) => theme.color.text.light};
  justify-content: flex-start;
`;

const Body = styled(Small)`
  margin: 0;
`;

class PendingStatus extends React.Component<PendingStatusProps, { time: number }> {
  public state = {
    time: new Date().getTime(),
  };

  private interval?: number;

  private handleInterval = () => {
    this.setState(() => ({
      time: new Date().getTime(),
    }));
  };

  public componentDidMount() {
    this.interval = window.setInterval(this.handleInterval, 10);
  }

  public componentWillUnmount() {
    window.clearInterval(this.interval);
  }

  public render() {
    const { startedAt, ...props } = this.props;
    const startedSince = this.state.time - startedAt;
    return (
      <Container {...props}>
        <Spinner left />
        <Body>
          {startedSince < 3000 ? "" : `Started ${formatDistance(new Date(this.state.time), new Date(startedAt))} ago`}
        </Body>
      </Container>
    );
  }
}

export default PendingStatus;
