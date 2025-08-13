"""Feature analysis tool for AI/ML readiness."""

from typing import Dict, Any, Optional, List
import structlog

from ..core.schema_manager import SchemaManager
from ..core.exceptions import ValidationError
from ..integrations import (
    CatalogAPI, MetadataAPI, BigQueryClient,
    MockCatalogAPI, MockMetadataAPI, MockBigQueryClient
)
from ..config import settings

logger = structlog.get_logger()


class FeatureAnalysisTool:
    """Tool for analyzing features for AI/ML readiness."""
    
    def __init__(self):
        # Initialize APIs based on mock setting
        if settings.use_mock_api:
            self.catalog_api = MockCatalogAPI()
            self.metadata_api = MockMetadataAPI()
            self.bigquery_client = MockBigQueryClient()
        else:
            self.catalog_api = CatalogAPI()
            self.metadata_api = MetadataAPI()
            self.bigquery_client = BigQueryClient()
            
        self.schema_manager = SchemaManager(self.catalog_api, self.metadata_api)
    
    async def run(
        self,
        org_id: str,
        use_case: str = "collaborative_filtering",
        columns: Optional[List[str]] = None,
        dataset_id: Optional[str] = None,
        table_id: Optional[str] = None,
        include_statistics: bool = True,
        include_quality: bool = True,
        include_correlations: bool = False
    ) -> Dict[str, Any]:
        """
        Run feature analysis for AI/ML readiness.
        
        Args:
            org_id: Organization ID
            use_case: ML use case (collaborative_filtering, churn_prediction, etc.)
            columns: Specific columns to analyze
            dataset_id: BigQuery dataset ID for statistical analysis
            table_id: BigQuery table ID for statistical analysis
            include_statistics: Include statistical analysis
            include_quality: Include data quality metrics
            include_correlations: Include correlation analysis
            
        Returns:
            Feature analysis results
        """
        try:
            # Connect to services
            await self.catalog_api.connect()
            await self.metadata_api.connect()
            await self.bigquery_client.connect()
            
            # Get schema if columns not specified
            if not columns:
                columns = await self._get_relevant_columns(org_id, use_case)
            
            # Base analysis
            analysis = {
                "org_id": org_id,
                "use_case": use_case,
                "analyzed_columns": len(columns),
                "features": {}
            }
            
            # Analyze each column
            for column in columns[:50]:  # Limit to 50 columns
                feature_info = await self._analyze_feature(
                    org_id, 
                    column,
                    include_statistics
                )
                analysis["features"][column] = feature_info
            
            # Add quality metrics if requested
            if include_quality and dataset_id and table_id:
                quality = await self.bigquery_client.get_data_quality_metrics(
                    dataset_id,
                    table_id,
                    columns[:20]  # Limit for performance
                )
                analysis["data_quality"] = quality
            
            # Add correlations if requested
            if include_correlations and dataset_id and table_id:
                numeric_cols = [
                    col for col, info in analysis["features"].items()
                    if info.get("data_type") in ["INTEGER", "FLOAT", "NUMERIC"]
                ][:10]  # Limit for performance
                
                if len(numeric_cols) >= 2:
                    correlations = await self.bigquery_client.calculate_correlation_matrix(
                        dataset_id,
                        table_id,
                        numeric_cols
                    )
                    analysis["correlations"] = correlations
            
            # Add readiness assessment
            analysis["readiness_assessment"] = self._assess_readiness(
                analysis,
                use_case
            )
            
            return analysis
            
        except Exception as e:
            logger.error(f"Feature analysis failed: {e}")
            return {
                "error": str(e),
                "org_id": org_id,
                "use_case": use_case
            }
        finally:
            # Cleanup
            await self.catalog_api.disconnect()
            await self.metadata_api.disconnect()
            await self.bigquery_client.disconnect()
    
    async def _get_relevant_columns(
        self, 
        org_id: str, 
        use_case: str
    ) -> List[str]:
        """Get relevant columns for use case."""
        schema = await self.schema_manager.get_schema(org_id)
        
        relevant_columns = []
        
        if use_case == "collaborative_filtering":
            # Need user, item, and interaction columns
            stores_to_check = ["profile_store", "event_store"]
            keywords = ["user", "product", "item", "purchase", "view", "rating"]
            
        elif use_case == "churn_prediction":
            # Need user behavior and engagement columns
            stores_to_check = ["profile_store", "event_store", "calculated_attribute"]
            keywords = ["user", "engagement", "activity", "last", "churn", "ltv"]
            
        elif use_case == "segmentation":
            # Need demographic and behavioral columns
            stores_to_check = ["profile_store", "calculated_attribute"]
            keywords = ["age", "gender", "location", "income", "segment", "cluster"]
            
        else:
            # Generic - get mix of columns
            stores_to_check = ["profile_store", "event_store", "calculated_attribute"]
            keywords = []
        
        # Collect columns from relevant stores
        for store in stores_to_check:
            columns = schema["stores"].get(store, [])
            for col in columns:
                col_name = col["name"].lower()
                # Check if column matches keywords or include all if no keywords
                if not keywords or any(kw in col_name for kw in keywords):
                    relevant_columns.append(col["name"])
        
        return relevant_columns[:100]  # Limit to 100 columns
    
    async def _analyze_feature(
        self, 
        org_id: str, 
        column_name: str,
        include_statistics: bool
    ) -> Dict[str, Any]:
        """Analyze individual feature."""
        # Get column metadata
        metadata = await self.metadata_api.get_column_metadata(
            org_id,
            [column_name]
        )
        
        col_metadata = metadata[0] if metadata else {}
        
        # Get column info from schema
        schema = await self.schema_manager.get_schema(org_id)
        col_info = None
        for attr in schema["raw_attributes"]:
            if attr["name"] == column_name:
                col_info = attr
                break
        
        if not col_info:
            return {"error": "Column not found in schema"}
        
        # Build feature analysis
        feature = {
            "name": column_name,
            "data_type": col_info.get("dataType", "UNKNOWN"),
            "attribute_type": col_info.get("attributeType", "UNKNOWN"),
            "is_pii": col_info.get("isRawPII", False),
            "store": self.schema_manager._determine_store(col_info)
        }
        
        if include_statistics and col_metadata:
            feature["statistics"] = {
                "unique_values": len(col_metadata.get("values", [])),
                "sample_values": col_metadata.get("values", [])[:5],
                "cardinality": self._determine_cardinality(
                    len(col_metadata.get("values", [])),
                    col_metadata.get("count", 1)
                )
            }
        
        # Determine feature type for ML
        feature["ml_feature_type"] = self._determine_ml_feature_type(
            feature["data_type"],
            feature.get("statistics", {}).get("cardinality", "UNKNOWN")
        )
        
        # Add feature engineering suggestions
        feature["engineering_suggestions"] = self._get_engineering_suggestions(
            feature
        )
        
        return feature
    
    def _determine_cardinality(self, unique_count: int, total_count: int) -> str:
        """Determine cardinality level."""
        if unique_count <= settings.LOW_CARDINALITY_THRESHOLD:
            return "LOW"
        elif unique_count / max(total_count, 1) > 0.95:
            return "VERY_HIGH"
        else:
            return "HIGH"
    
    def _determine_ml_feature_type(
        self, 
        data_type: str, 
        cardinality: str
    ) -> str:
        """Determine ML feature type."""
        if data_type in ["INTEGER", "FLOAT", "NUMERIC"]:
            return "NUMERICAL"
        elif data_type == "BOOLEAN":
            return "BINARY"
        elif data_type in ["STRING", "VARCHAR"] and cardinality == "LOW":
            return "CATEGORICAL"
        elif data_type in ["STRING", "VARCHAR"] and cardinality in ["HIGH", "VERY_HIGH"]:
            return "TEXT"
        elif data_type in ["TIMESTAMP", "DATE", "DATETIME"]:
            return "TEMPORAL"
        else:
            return "UNKNOWN"
    
    def _get_engineering_suggestions(self, feature: Dict[str, Any]) -> List[str]:
        """Get feature engineering suggestions."""
        suggestions = []
        
        ml_type = feature.get("ml_feature_type", "UNKNOWN")
        
        if ml_type == "NUMERICAL":
            suggestions.extend([
                "Consider normalization or standardization",
                "Check for outliers and handle appropriately",
                "Create buckets/bins for tree-based models"
            ])
        elif ml_type == "CATEGORICAL":
            suggestions.extend([
                "Use one-hot encoding for low cardinality",
                "Consider target encoding for high cardinality",
                "Create interaction features with other categoricals"
            ])
        elif ml_type == "TEXT":
            suggestions.extend([
                "Extract text features (length, word count)",
                "Use TF-IDF or word embeddings",
                "Consider topic modeling"
            ])
        elif ml_type == "TEMPORAL":
            suggestions.extend([
                "Extract date components (year, month, day, hour)",
                "Calculate time differences and intervals",
                "Create cyclical features for periodic patterns"
            ])
        
        if feature.get("is_pii"):
            suggestions.append("Apply privacy-preserving techniques (hashing, anonymization)")
        
        return suggestions
    
    def _assess_readiness(
        self, 
        analysis: Dict[str, Any], 
        use_case: str
    ) -> Dict[str, Any]:
        """Assess overall readiness for use case."""
        readiness = {
            "score": 0,
            "status": "NOT_READY",
            "strengths": [],
            "gaps": [],
            "recommendations": []
        }
        
        features = analysis.get("features", {})
        
        if use_case == "collaborative_filtering":
            # Check for required features
            has_user = any("user" in f.lower() for f in features.keys())
            has_item = any("product" in f.lower() or "item" in f.lower() for f in features.keys())
            has_interaction = any(
                "purchase" in f.lower() or "view" in f.lower() or "rating" in f.lower() 
                for f in features.keys()
            )
            
            if has_user:
                readiness["strengths"].append("User identifiers found")
                readiness["score"] += 30
            else:
                readiness["gaps"].append("No user identifiers found")
                
            if has_item:
                readiness["strengths"].append("Item/product identifiers found")
                readiness["score"] += 30
            else:
                readiness["gaps"].append("No item/product identifiers found")
                
            if has_interaction:
                readiness["strengths"].append("User-item interactions found")
                readiness["score"] += 40
            else:
                readiness["gaps"].append("No user-item interaction data found")
                
        # Check data quality if available
        if "data_quality" in analysis:
            quality_scores = []
            for col, metrics in analysis["data_quality"].items():
                if "completeness" in metrics:
                    quality_scores.append(metrics["completeness"])
                    
            if quality_scores:
                avg_quality = sum(quality_scores) / len(quality_scores)
                if avg_quality >= 90:
                    readiness["strengths"].append(f"High data quality ({avg_quality:.1f}% completeness)")
                elif avg_quality < 70:
                    readiness["gaps"].append(f"Low data quality ({avg_quality:.1f}% completeness)")
                    readiness["recommendations"].append("Improve data quality before model training")
        
        # Determine overall status
        if readiness["score"] >= 80:
            readiness["status"] = "READY"
        elif readiness["score"] >= 60:
            readiness["status"] = "PARTIALLY_READY"
        else:
            readiness["status"] = "NOT_READY"
            
        # Add general recommendations
        if readiness["status"] != "READY":
            readiness["recommendations"].extend([
                "Identify and collect missing feature types",
                "Ensure sufficient data volume for training",
                "Validate data consistency across stores"
            ])
        
        return readiness