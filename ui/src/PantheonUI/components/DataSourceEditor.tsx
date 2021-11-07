import { CardColumn, CardColumns, CardItem, Form, Input, styled, Textarea } from "@operational/components";
import get from "lodash/get";
import omit from "lodash/omit";
import pick from "lodash/pick";
import React from "react";

import { DataSource } from "../data/dataSource";
import { DataSourceProductProperty } from "../data/dataSourceProduct";
import KeyValueEditor from "./KeyValueEditor";

export interface BaseProps {
  dataSource: Partial<DataSource>;
  readOnly?: boolean;
  onChange: (dataSource: BaseProps["dataSource"]) => void;
  fieldErrors?: { [key: string]: string };
}

const TwoColumnsForm = styled(Form)`
  display: grid;
  grid-template-columns: 400px 1fr;
  grid-column-gap: 40px;
`;

export type DataSourceInfoEditorProps = BaseProps;

export const DataSourceInfoEditor: React.SFC<DataSourceInfoEditorProps> = props =>
  props.readOnly ? (
    <Form>
      <CardItem title="Name" value={props.dataSource.name} />
      <CardItem title="Description" value={props.dataSource.description} />
    </Form>
  ) : (
    <TwoColumnsForm onSubmit={e => e.preventDefault()}>
      <div>
        <Input
          fullWidth
          label="Name"
          data-cy="pantheon--data-source-wizard__name_input"
          value={props.dataSource.name}
          error={get(props, "fieldErrors.name")}
          onChange={newName => {
            props.onChange({
              ...props.dataSource,
              name: newName,
            });
          }}
        />
        <Textarea
          fullWidth
          label="Description"
          data-cy="pantheon--data-source-wizard__description-input"
          value={props.dataSource.description ? props.dataSource.description : ""}
          error={get(props, "fieldErrors.description")}
          onChange={newDescription => {
            props.onChange({
              ...props.dataSource,
              description: newDescription,
            });
          }}
        />
      </div>
      <div />
    </TwoColumnsForm>
  );

export interface DataSourceConnectionEditorProps extends BaseProps {
  properties: DataSourceProductProperty[];
  imageUrl?: string;
}

export const DataSourceConnectionEditor: React.SFC<DataSourceConnectionEditorProps> = props => {
  const properties = props.dataSource.properties || {};
  const additionalProperties = omit(properties, props.properties.map(property => property.name));
  return (
    <>
      {props.readOnly ? (
        <Form>
          {props.properties.map((property, index) => (
            <CardItem key={index} title={property.name} value={properties[property.name] || ""} />
          ))}

          {Object.entries(additionalProperties).length > 0 && (
            <CardItem
              key={"Custom parameters"}
              title={"Custom parameters"}
              value={Object.entries(additionalProperties).map(([name, value]) => (
                <div key={name}>
                  {name}:{value}
                </div>
              ))}
            />
          )}
          {props.children}
        </Form>
      ) : (
        <TwoColumnsForm onSubmit={e => e.preventDefault()}>
          <div>
            {props.properties.map((property, index) => (
              <Input
                fullWidth
                error={get(props, `fieldErrors.${property.name}`)}
                key={index}
                data-cy={`pantheon--data-source-wizard__field-${property.name[0].toUpperCase() +
                  property.name.slice(1)}`}
                label={property.name[0].toUpperCase() + property.name.slice(1)}
                value={properties[property.name] || ""}
                onChange={newValue => {
                  props.onChange({
                    ...props.dataSource,
                    properties: {
                      ...properties,
                      [property.name]: newValue,
                    },
                  });
                }}
              />
            ))}
            {props.children}
          </div>
          <CardColumn title="Custom parameters" noPadding>
            <KeyValueEditor
              data={additionalProperties}
              onChange={newData => {
                props.onChange({
                  ...props.dataSource,
                  properties: {
                    ...pick(properties, props.properties.map(prop => prop.name)),
                    ...newData,
                  },
                });
              }}
            />
          </CardColumn>
        </TwoColumnsForm>
      )}
    </>
  );
};

export interface DataSourceEditorProps {
  productProps: DataSourceProductProperty[];
  dataSource: Partial<DataSource>;
  productImgUrl: string | undefined;
  productName: string;
  children?: React.ReactNode;
  onChange: BaseProps["onChange"];
  fieldErrors?: { [key: string]: string };
}

export const DataSourceEditor: React.SFC<DataSourceEditorProps> = props => (
  <CardColumns>
    <CardColumn title="General information" fullWidth>
      <DataSourceInfoEditor onChange={props.onChange} dataSource={props.dataSource} fieldErrors={props.fieldErrors} />
    </CardColumn>
    <CardColumn title={`Connection to ${props.productName}`} fullWidth>
      <DataSourceConnectionEditor
        fieldErrors={props.fieldErrors}
        onChange={props.onChange}
        dataSource={props.dataSource}
        properties={props.productProps}
        imageUrl={props.productImgUrl}
        children={props.children}
      />
    </CardColumn>
  </CardColumns>
);
