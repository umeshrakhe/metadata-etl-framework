import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Set, Tuple
from dataclasses import dataclass, asdict
import uuid
import json

logger = logging.getLogger("DataLineageTracker")


@dataclass
class SourceInfo:
    """Information about a data source."""
    system_type: str  # database, file, api, etc.
    connection_name: str
    database_name: Optional[str] = None
    schema_name: Optional[str] = None
    table_name: Optional[str] = None
    columns: List[str] = None
    filter_condition: Optional[str] = None
    record_count: Optional[int] = None

    def __post_init__(self):
        if self.columns is None:
            self.columns = []


@dataclass
class TargetInfo:
    """Information about a data target."""
    system_type: str
    connection_name: str
    database_name: Optional[str] = None
    schema_name: Optional[str] = None
    table_name: Optional[str] = None
    columns: List[str] = None
    record_count: Optional[int] = None

    def __post_init__(self):
        if self.columns is None:
            self.columns = []


@dataclass
class TransformationInfo:
    """Information about a transformation."""
    transform_id: str
    transform_type: str  # filter, join, aggregate, etc.
    input_columns: List[str]
    output_columns: List[str]
    transformation_logic: str
    parameters: Dict[str, Any] = None
    execution_order: int = 0

    def __post_init__(self):
        if self.parameters is None:
            self.parameters = {}


@dataclass
class ColumnLineage:
    """Column-level lineage information."""
    target_table: str
    target_column: str
    source_table: str
    source_column: str
    transformation_logic: str
    transformation_type: str
    run_id: str
    created_at: datetime


@dataclass
class LineageGraph:
    """Graph structure for lineage visualization."""
    nodes: List[Dict[str, Any]]
    edges: List[Dict[str, Any]]
    metadata: Dict[str, Any]


class DataLineageTracker:
    """
    Comprehensive data lineage tracking for ETL pipelines.
    Tracks data flow from source to target, column-level lineage, and supports impact analysis.
    """

    def __init__(self, db_connection: Any = None):
        self.db = db_connection
        self.logger = logging.getLogger(self.__class__.__name__)
        self.current_lineage: Dict[str, List[Dict[str, Any]]] = {}  # run_id -> lineage steps

    def track_source_to_target(self, run_id: str, source_info: SourceInfo, target_info: TargetInfo) -> str:
        """Record data flow from source to target."""
        lineage_id = str(uuid.uuid4())

        if run_id not in self.current_lineage:
            self.current_lineage[run_id] = []

        lineage_step = {
            "lineage_id": lineage_id,
            "run_id": run_id,
            "step_type": "source_to_target",
            "source_info": asdict(source_info),
            "target_info": asdict(target_info),
            "timestamp": datetime.utcnow(),
            "transformations": []
        }

        self.current_lineage[run_id].append(lineage_step)
        self._store_lineage_step(lineage_step)

        self.logger.info(f"Tracked source-to-target flow for run {run_id}: "
                        f"{source_info.table_name} -> {target_info.table_name}")
        return lineage_id

    def track_transformation(self, run_id: str, transform_id: str, input_columns: List[str],
                           output_columns: List[str], transformation_logic: str = "",
                           transform_type: str = "custom", parameters: Dict[str, Any] = None) -> None:
        """Record transformation with column lineage."""
        if run_id not in self.current_lineage:
            self.logger.warning(f"No active lineage tracking for run {run_id}")
            return

        transformation_info = TransformationInfo(
            transform_id=transform_id,
            transform_type=transform_type,
            input_columns=input_columns,
            output_columns=output_columns,
            transformation_logic=transformation_logic,
            parameters=parameters or {},
            execution_order=len(self.current_lineage[run_id])
        )

        # Add to current lineage
        current_step = self.current_lineage[run_id][-1]
        current_step["transformations"].append(asdict(transformation_info))

        # Store column-level lineage
        for output_col in output_columns:
            column_lineage = ColumnLineage(
                target_table=current_step["target_info"]["table_name"],
                target_column=output_col,
                source_table=current_step["source_info"]["table_name"],
                source_column=input_columns[0] if len(input_columns) == 1 else ",".join(input_columns),
                transformation_logic=transformation_logic,
                transformation_type=transform_type,
                run_id=run_id,
                created_at=datetime.utcnow()
            )
            self._store_column_lineage(column_lineage)

        # Update stored lineage
        self._update_lineage_step(current_step)

        self.logger.info(f"Tracked transformation {transform_id} for run {run_id}: "
                        f"{input_columns} -> {output_columns}")

    def record_column_derivation(self, run_id: str, target_table: str, target_column: str,
                                source_columns: List[Tuple[str, str]], transformation_logic: str,
                                transform_type: str = "derived") -> None:
        """Record detailed column derivation with multiple sources."""
        for source_table, source_column in source_columns:
            column_lineage = ColumnLineage(
                target_table=target_table,
                target_column=target_column,
                source_table=source_table,
                source_column=source_column,
                transformation_logic=transformation_logic,
                transformation_type=transform_type,
                run_id=run_id,
                created_at=datetime.utcnow()
            )
            self._store_column_lineage(column_lineage)

        self.logger.info(f"Recorded column derivation for {target_table}.{target_column} "
                        f"from {len(source_columns)} source columns")

    def get_upstream_dependencies(self, table_name: str, column_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """Find upstream data sources for a table/column."""
        if not self.db:
            raise ValueError("Database connection required for dependency analysis")

        try:
            if column_name:
                sql = """
                    SELECT DISTINCT
                        cl.source_table, cl.source_column, cl.transformation_logic,
                        cl.transformation_type, cl.run_id, cl.created_at,
                        ls.source_info, ls.target_info
                    FROM COLUMN_LINEAGE cl
                    JOIN LINEAGE_STEPS ls ON cl.run_id = ls.run_id
                    WHERE cl.target_table = %s AND cl.target_column = %s
                    ORDER BY cl.created_at DESC
                """
                params = (table_name, column_name)
            else:
                sql = """
                    SELECT DISTINCT
                        cl.source_table, cl.source_column, cl.transformation_logic,
                        cl.transformation_type, cl.run_id, cl.created_at,
                        ls.source_info, ls.target_info
                    FROM COLUMN_LINEAGE cl
                    JOIN LINEAGE_STEPS ls ON cl.run_id = ls.run_id
                    WHERE cl.target_table = %s
                    ORDER BY cl.created_at DESC
                """
                params = (table_name,)

            self.db.execute(sql, params)
            rows = self.db.fetchall()

            dependencies = []
            for row in rows:
                dependencies.append({
                    "source_table": row[0],
                    "source_column": row[1],
                    "transformation_logic": row[2],
                    "transformation_type": row[3],
                    "run_id": row[4],
                    "created_at": row[5],
                    "source_info": json.loads(row[6]) if row[6] else None,
                    "target_info": json.loads(row[7]) if row[7] else None
                })

            return dependencies

        except Exception as e:
            self.logger.exception(f"Failed to get upstream dependencies: {e}")
            raise

    def get_downstream_dependencies(self, table_name: str, column_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """Find downstream data consumers for a table/column."""
        if not self.db:
            raise ValueError("Database connection required for dependency analysis")

        try:
            if column_name:
                sql = """
                    SELECT DISTINCT
                        cl.target_table, cl.target_column, cl.transformation_logic,
                        cl.transformation_type, cl.run_id, cl.created_at,
                        ls.source_info, ls.target_info
                    FROM COLUMN_LINEAGE cl
                    JOIN LINEAGE_STEPS ls ON cl.run_id = ls.run_id
                    WHERE cl.source_table = %s AND cl.source_column = %s
                    ORDER BY cl.created_at DESC
                """
                params = (table_name, column_name)
            else:
                sql = """
                    SELECT DISTINCT
                        cl.target_table, cl.target_column, cl.transformation_logic,
                        cl.transformation_type, cl.run_id, cl.created_at,
                        ls.source_info, ls.target_info
                    FROM COLUMN_LINEAGE cl
                    JOIN LINEAGE_STEPS ls ON cl.run_id = ls.run_id
                    WHERE cl.source_table = %s
                    ORDER BY cl.created_at DESC
                """
                params = (table_name,)

            self.db.execute(sql, params)
            rows = self.db.fetchall()

            dependencies = []
            for row in rows:
                dependencies.append({
                    "target_table": row[0],
                    "target_column": row[1],
                    "transformation_logic": row[2],
                    "transformation_type": row[3],
                    "run_id": row[4],
                    "created_at": row[5],
                    "source_info": json.loads(row[6]) if row[6] else None,
                    "target_info": json.loads(row[7]) if row[7] else None
                })

            return dependencies

        except Exception as e:
            self.logger.exception(f"Failed to get downstream dependencies: {e}")
            raise

    def trace_data_flow(self, run_id: str) -> List[Dict[str, Any]]:
        """Reconstruct complete pipeline flow for a run."""
        if run_id not in self.current_lineage:
            # Try to load from database
            if self.db:
                return self._load_lineage_from_db(run_id)
            else:
                raise ValueError(f"No lineage data available for run {run_id}")

        return self.current_lineage[run_id]

    def generate_lineage_graph(self, pipeline_id: str) -> LineageGraph:
        """Create graph structure for lineage visualization."""
        if not self.db:
            raise ValueError("Database connection required for graph generation")

        try:
            # Get all lineage steps for the pipeline
            sql = """
                SELECT ls.run_id, ls.source_info, ls.target_info, ls.transformations,
                       ls.timestamp, ee.pipeline_id
                FROM LINEAGE_STEPS ls
                JOIN ETL_EXECUTION ee ON ls.run_id = ee.run_id
                WHERE ee.pipeline_id = %s
                ORDER BY ls.timestamp
            """

            self.db.execute(sql, (pipeline_id,))
            rows = self.db.fetchall()

            nodes = []
            edges = []
            node_ids = set()

            for row in rows:
                run_id, source_info_json, target_info_json, transformations_json, timestamp, pid = row

                source_info = json.loads(source_info_json)
                target_info = json.loads(target_info_json)
                transformations = json.loads(transformations_json) if transformations_json else []

                # Add source node
                source_node_id = f"{source_info['system_type']}.{source_info.get('table_name', 'unknown')}"
                if source_node_id not in node_ids:
                    nodes.append({
                        "id": source_node_id,
                        "label": source_info.get('table_name', 'Unknown Source'),
                        "type": "source",
                        "system_type": source_info['system_type'],
                        "properties": source_info
                    })
                    node_ids.add(source_node_id)

                # Add target node
                target_node_id = f"{target_info['system_type']}.{target_info.get('table_name', 'unknown')}"
                if target_node_id not in node_ids:
                    nodes.append({
                        "id": target_node_id,
                        "label": target_info.get('table_name', 'Unknown Target'),
                        "type": "target",
                        "system_type": target_info['system_type'],
                        "properties": target_info
                    })
                    node_ids.add(target_node_id)

                # Add transformation nodes and edges
                for i, transform in enumerate(transformations):
                    transform_node_id = f"{run_id}_transform_{i}"
                    if transform_node_id not in node_ids:
                        nodes.append({
                            "id": transform_node_id,
                            "label": f"{transform['transform_type']} ({transform['transform_id']})",
                            "type": "transformation",
                            "properties": transform
                        })
                        node_ids.add(transform_node_id)

                    # Add edges
                    edges.append({
                        "from": source_node_id,
                        "to": transform_node_id,
                        "label": f"input: {', '.join(transform['input_columns'])}",
                        "type": "data_flow"
                    })
                    edges.append({
                        "from": transform_node_id,
                        "to": target_node_id,
                        "label": f"output: {', '.join(transform['output_columns'])}",
                        "type": "data_flow"
                    })

                # If no transformations, direct edge
                if not transformations:
                    edges.append({
                        "from": source_node_id,
                        "to": target_node_id,
                        "label": "direct flow",
                        "type": "data_flow"
                    })

            metadata = {
                "pipeline_id": pipeline_id,
                "generated_at": datetime.utcnow().isoformat(),
                "total_nodes": len(nodes),
                "total_edges": len(edges)
            }

            return LineageGraph(nodes=nodes, edges=edges, metadata=metadata)

        except Exception as e:
            self.logger.exception(f"Failed to generate lineage graph: {e}")
            raise

    def get_column_lineage(self, target_table: str, target_column: str) -> Dict[str, Any]:
        """Show complete column lineage with all transformations."""
        upstream = self.get_upstream_dependencies(target_table, target_column)

        lineage_info = {
            "target_table": target_table,
            "target_column": target_column,
            "upstream_sources": upstream,
            "transformation_chain": [],
            "data_provenance": []
        }

        # Build transformation chain
        current_table = target_table
        current_column = target_column

        while upstream:
            source = upstream[0]  # Take the most recent
            lineage_info["transformation_chain"].append({
                "source_table": source["source_table"],
                "source_column": source["source_column"],
                "transformation_type": source["transformation_type"],
                "transformation_logic": source["transformation_logic"],
                "run_id": source["run_id"]
            })

            # Move up the chain
            current_table = source["source_table"]
            current_column = source["source_column"]
            upstream = self.get_upstream_dependencies(current_table, current_column)

        return lineage_info

    def analyze_impact(self, source_change: Dict[str, Any]) -> Dict[str, Any]:
        """Determine affected downstream assets from a source change."""
        change_type = source_change.get("change_type")  # table_change, column_change, etc.
        table_name = source_change.get("table_name")
        column_name = source_change.get("column_name")

        affected_assets = {
            "directly_affected": [],
            "indirectly_affected": [],
            "impact_level": "low",
            "recommendations": []
        }

        try:
            # Get downstream dependencies
            downstream = self.get_downstream_dependencies(table_name, column_name)

            if downstream:
                affected_assets["directly_affected"] = [
                    {"table": d["target_table"], "column": d["target_column"], "run_id": d["run_id"]}
                    for d in downstream
                ]

                # Check for indirect impacts (tables that depend on affected tables)
                for direct in downstream:
                    indirect = self.get_downstream_dependencies(direct["target_table"])
                    affected_assets["indirectly_affected"].extend([
                        {"table": i["target_table"], "column": i["target_column"], "run_id": i["run_id"]}
                        for i in indirect
                    ])

                # Determine impact level
                total_affected = len(affected_assets["directly_affected"]) + len(affected_assets["indirectly_affected"])
                if total_affected > 10:
                    affected_assets["impact_level"] = "high"
                elif total_affected > 5:
                    affected_assets["impact_level"] = "medium"
                else:
                    affected_assets["impact_level"] = "low"

                # Generate recommendations
                if affected_assets["impact_level"] == "high":
                    affected_assets["recommendations"].append("Schedule downtime for impact assessment")
                    affected_assets["recommendations"].append("Notify all downstream system owners")
                elif affected_assets["impact_level"] == "medium":
                    affected_assets["recommendations"].append("Test downstream systems after change")
                    affected_assets["recommendations"].append("Monitor data quality post-deployment")

        except Exception as e:
            self.logger.exception(f"Failed to analyze impact: {e}")

        return affected_assets

    def get_data_provenance(self, record_id: Any, table_name: str) -> Dict[str, Any]:
        """Trace the origin of a specific record."""
        # This would typically require record-level tracking
        # For now, return table-level provenance
        provenance = {
            "record_id": record_id,
            "table_name": table_name,
            "upstream_sources": self.get_upstream_dependencies(table_name),
            "transformation_history": [],
            "data_quality_checks": [],
            "last_updated": datetime.utcnow().isoformat()
        }

        return provenance

    def store_lineage_metadata(self, lineage_info: Dict[str, Any]) -> None:
        """Persist lineage information to database."""
        if not self.db:
            self.logger.warning("No database connection - lineage not persisted")
            return

        try:
            # This would store additional metadata beyond the basic lineage steps
            sql = """
                INSERT INTO LINEAGE_METADATA (
                    metadata_id, lineage_type, metadata_info, created_at
                ) VALUES (%s, %s, %s, %s)
            """

            metadata_id = str(uuid.uuid4())
            params = (
                metadata_id,
                lineage_info.get("type", "general"),
                json.dumps(lineage_info),
                datetime.utcnow()
            )

            self.db.begin()
            self.db.execute(sql, params)
            self.db.commit()

            self.logger.debug("Stored lineage metadata")

        except Exception as e:
            self.db.rollback()
            self.logger.exception(f"Failed to store lineage metadata: {e}")

    def export_lineage_for_governance(self, pipeline_id: str, format: str = "json") -> str:
        """Export lineage data for governance tools."""
        if not self.db:
            raise ValueError("Database connection required for export")

        try:
            graph = self.generate_lineage_graph(pipeline_id)

            if format == "json":
                export_data = {
                    "pipeline_id": pipeline_id,
                    "export_timestamp": datetime.utcnow().isoformat(),
                    "format": "data_catalog_v1",
                    "nodes": graph.nodes,
                    "edges": graph.edges,
                    "metadata": graph.metadata
                }
                return json.dumps(export_data, indent=2, default=str)

            elif format == "alation":
                # Format for Alation data catalog
                alation_data = {
                    "dataflow": {
                        "id": pipeline_id,
                        "name": f"ETL Pipeline {pipeline_id}",
                        "nodes": [
                            {
                                "id": node["id"],
                                "type": node["type"],
                                "properties": node["properties"]
                            } for node in graph.nodes
                        ],
                        "edges": graph.edges
                    }
                }
                return json.dumps(alation_data, indent=2, default=str)

            elif format == "collibra":
                # Format for Collibra data governance
                collibra_data = {
                    "assets": graph.nodes,
                    "relations": graph.edges,
                    "metadata": graph.metadata
                }
                return json.dumps(collibra_data, indent=2, default=str)

            else:
                raise ValueError(f"Unsupported export format: {format}")

        except Exception as e:
            self.logger.exception(f"Failed to export lineage: {e}")
            raise

    def _store_lineage_step(self, lineage_step: Dict[str, Any]) -> None:
        """Store a lineage step in the database."""
        if not self.db:
            return

        try:
            sql = """
                INSERT INTO LINEAGE_STEPS (
                    lineage_id, run_id, step_type, source_info, target_info,
                    transformations, timestamp, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """

            params = (
                lineage_step["lineage_id"],
                lineage_step["run_id"],
                lineage_step["step_type"],
                json.dumps(lineage_step["source_info"]),
                json.dumps(lineage_step["target_info"]),
                json.dumps(lineage_step["transformations"]),
                lineage_step["timestamp"],
                datetime.utcnow()
            )

            self.db.begin()
            self.db.execute(sql, params)
            self.db.commit()

        except Exception as e:
            self.db.rollback()
            self.logger.exception(f"Failed to store lineage step: {e}")

    def _update_lineage_step(self, lineage_step: Dict[str, Any]) -> None:
        """Update a lineage step in the database."""
        if not self.db:
            return

        try:
            sql = """
                UPDATE LINEAGE_STEPS
                SET transformations = %s, updated_at = %s
                WHERE lineage_id = %s
            """

            params = (
                json.dumps(lineage_step["transformations"]),
                datetime.utcnow(),
                lineage_step["lineage_id"]
            )

            self.db.begin()
            self.db.execute(sql, params)
            self.db.commit()

        except Exception as e:
            self.db.rollback()
            self.logger.exception(f"Failed to update lineage step: {e}")

    def _store_column_lineage(self, column_lineage: ColumnLineage) -> None:
        """Store column-level lineage."""
        if not self.db:
            return

        try:
            sql = """
                INSERT INTO COLUMN_LINEAGE (
                    lineage_id, target_table, target_column, source_table, source_column,
                    transformation_logic, transformation_type, run_id, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            lineage_id = str(uuid.uuid4())
            params = (
                lineage_id,
                column_lineage.target_table,
                column_lineage.target_column,
                column_lineage.source_table,
                column_lineage.source_column,
                column_lineage.transformation_logic,
                column_lineage.transformation_type,
                column_lineage.run_id,
                column_lineage.created_at
            )

            self.db.begin()
            self.db.execute(sql, params)
            self.db.commit()

        except Exception as e:
            self.db.rollback()
            self.logger.exception(f"Failed to store column lineage: {e}")

    def _load_lineage_from_db(self, run_id: str) -> List[Dict[str, Any]]:
        """Load lineage data from database."""
        if not self.db:
            return []

        try:
            sql = """
                SELECT lineage_id, step_type, source_info, target_info, transformations, timestamp
                FROM LINEAGE_STEPS
                WHERE run_id = %s
                ORDER BY timestamp
            """

            self.db.execute(sql, (run_id,))
            rows = self.db.fetchall()

            lineage_steps = []
            for row in rows:
                lineage_steps.append({
                    "lineage_id": row[0],
                    "run_id": run_id,
                    "step_type": row[1],
                    "source_info": json.loads(row[2]) if row[2] else {},
                    "target_info": json.loads(row[3]) if row[3] else {},
                    "transformations": json.loads(row[4]) if row[4] else [],
                    "timestamp": row[5]
                })

            return lineage_steps

        except Exception as e:
            self.logger.exception(f"Failed to load lineage from database: {e}")
            return []