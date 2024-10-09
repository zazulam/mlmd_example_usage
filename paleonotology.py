from typing import Any, List

import ml_metadata
from ml_metadata.proto import metadata_store_pb2
from ml_metadata.proto.metadata_store_pb2 import Artifact, Execution, LineageGraph
from google.protobuf import json_format

# lineage details
# https://github.com/google/ml-metadata/blob/master/ml_metadata/proto/metadata_store.proto#L476


class KFPaleontologist():
    def __init__(self, conn_config: dict) -> None:
        self.mlmd = ml_metadata.MetadataStore(conn_config)

    # get all artifacts associated with a KFP run
    def get_artifacts_from_run(self, run_id: str) -> List[Artifact]:
        container_executions = self.get_executions(run_id)

        container_execution_ids = [container.id for container in container_executions]

        events = self.mlmd.get_events_by_execution_ids(
            execution_ids=container_execution_ids
        )

        artifact_ids = [event.artifact_id for event in events]

        artifacts = self.mlmd.get_artifacts_by_id(artifact_ids)

        # Question on whether or not to format the protobuf message to a dictionary
        # json_artifacts = [json_format.MessageToDict(artifact) for artifact in artifacts]
        
        return artifacts
    
    
    # get all executions associated with a KFP run
    def get_executions(self, run_id: str) -> List[Execution]:
        run_executions = self.mlmd.get_executions(
            list_options = ml_metadata.ListOptions(
                filter_query=f'name="run/{run_id}"'
            )
        )

        run_execution_id = run_executions[0].id
               
        container_executions = self.mlmd.get_executions(
            list_options = ml_metadata.ListOptions(
                filter_query=f'custom_properties.parent_dag_id.int_value={run_execution_id}'
            )
        )

        all_run_executions = [run_executions[0]] + container_executions
        
        return all_run_executions


    # get all artifacts associated with an execution from a KFP run
    def get_artifacts_from_execution(self, execution_id: str) -> List[Artifact]:
        events = self.mlmd.get_events_by_execution_ids([execution_id])
        
        artifact_ids = [event.artifact_id for event in events]
        
        artifacts = self.mlmd.get_artifacts_by_id(artifact_ids)

        return artifacts
   
    
    # get all executions associated with an artifact from a KFP run
    def get_artifact_execution_history(self, artifact_id: str) -> List[Execution]:
        events = self.mlmd.get_events_by_artifact_ids([artifact_id])
        
        execution_ids = [event.execution_id for event in events]

        executions = self.mlmd.get_executions_by_id(execution_ids)

        return executions


    def get_artifact_lineage(self, artifact_id: int) -> LineageGraph:
        lineage = self.mlmd.get_lineage_subgraph(
            query_options= metadata_store_pb2.LineageSubgraphQueryOptions(
                starting_artifacts = metadata_store_pb2.LineageSubgraphQueryOptions.StartingNodes(
                filter_query=f"id = {artifact_id}"
            ))
        )

        return lineage
    

    def get_artifact_by_id(self, artifact_id: int) -> Artifact:
        artifacts = self.mlmd.get_artifacts_by_id([artifact_id])
        return artifacts[0]
    
    
    def get_artifact_by_uri(self, artifact_uri: str) -> Artifact:
        artifacts = self.mlmd.get_artifacts_by_uri(artifact_uri)
        return artifacts[0]

    
    def get_execution_by_id(self, execution_id: int) -> Execution:
        executions = self.mlmd.get_executions_by_id([execution_id])
        return executions[0]
   
   
    def get_artifacts_by_type(self, artifact_type: str) -> List[Artifact]:
        artifacts = self.mlmd.get_artifacts_by_type(artifact_type)
        return artifacts


    def get_executions_by_type(self, execution_type: str) -> List[Execution]:
        executions = self.mlmd.get_executions_by_type(execution_type)
        return executions


    def get_artifacts_by_custom_property(self, custom_property: str, value: Any) -> List[Artifact]:
        property_type = type(value)
        if property_type == str:
            property_type = 'string'
        else:
            property_type = property_type.__name__.lower()

        artifacts = self.mlmd.get_artifacts(
            list_options = ml_metadata.ListOptions(
                filter_query=f'custom_properties.{custom_property}.{property_type}_value="{value}"'
            )
        )

        return artifacts


    def get_executions_by_custom_property(self, custom_property: str, value: Any) -> List[Execution]:
        property_type = type(value)
        if property_type == str:
            property_type = 'string'
        else:
            property_type = property_type.__name__.lower()

        executions = self.mlmd.get_executions(
            list_options = ml_metadata.ListOptions(
                filter_query=f'custom_properties.{custom_property}.{property_type}_value="{value}"'
            )
        )

        return executions

    def visualize_lineage(self, lineage: LineageGraph, display_association: bool = False, display_attribution: bool =False) -> None:
        from graphviz import Digraph
            
        # Create a directed graph using Graphviz
        dot = Digraph(comment="MLMD Lineage Graph")

        # Add nodes for artifacts and executions
        for artifact in lineage.artifacts:
            dot.node(f"Artifact-{artifact.id}", f"Artifact: {artifact.uri}", shape="box", style="filled", color="lightblue")

        for execution in lineage.executions:
            dot.node(f"Execution-{execution.id}", f"{execution.type}: {execution.id}", shape="ellipse", style="filled", color="lightgreen")

        # Add edges based on events
        for event in lineage.events:
            if event.type == metadata_store_pb2.Event.INPUT:
                dot.edge(f"Artifact-{event.artifact_id}", f"Execution-{event.execution_id}", label="input")
            elif event.type == metadata_store_pb2.Event.OUTPUT:
                dot.edge(f"Execution-{event.execution_id}", f"Artifact-{event.artifact_id}", label="output")
        for context in lineage.contexts:
            dot.node(f"Context-{context.id}", f"Context: {context.name}", shape="ellipse", style="filled", color="lightyellow")
        if display_attribution:
            for attribution in lineage.attributions:
                dot.edge(f"Artifact-{attribution.artifact_id}", f"Context-{attribution.context_id}", label="attribution")
        if display_association:
            for association in lineage.associations:
                dot.edge(f"Execution-{association.execution_id}", f"Context-{association.context_id}", label="association")
        # Render and visualize the graph
        dot.render('lineage_graph', view=True)
        

        