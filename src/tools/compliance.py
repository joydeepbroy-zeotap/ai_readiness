"""Compliance checker tool for data privacy and regulations."""

from typing import Dict, Any, Optional, List
import structlog
from datetime import datetime

from ..core.schema_manager import SchemaManager
from ..core.exceptions import ValidationError
from ..integrations import (
    CatalogAPI, MetadataAPI,
    MockCatalogAPI, MockMetadataAPI
)
from ..config import settings, PII_PATTERNS

logger = structlog.get_logger()


class ComplianceCheckerTool:
    """Tool for checking data compliance and privacy requirements."""
    
    def __init__(self):
        # Initialize APIs based on mock setting
        if settings.use_mock_api:
            self.catalog_api = MockCatalogAPI()
            self.metadata_api = MockMetadataAPI()
        else:
            self.catalog_api = CatalogAPI()
            self.metadata_api = MetadataAPI()
            
        self.schema_manager = SchemaManager(self.catalog_api, self.metadata_api)
    
    async def run(
        self,
        org_id: str,
        check_type: str = "full",
        regulations: Optional[List[str]] = None,
        columns: Optional[List[str]] = None,
        generate_report: bool = True
    ) -> Dict[str, Any]:
        """
        Run compliance checks on data.
        
        Args:
            org_id: Organization ID
            check_type: Type of check (full, pii, consent, retention)
            regulations: Specific regulations to check (GDPR, CCPA, etc.)
            columns: Specific columns to check
            generate_report: Whether to generate detailed report
            
        Returns:
            Compliance check results
        """
        try:
            # Connect to APIs
            await self.catalog_api.connect()
            await self.metadata_api.connect()
            
            # Default regulations if not specified
            if not regulations:
                regulations = ["GDPR", "CCPA"]
            
            # Get schema
            schema = await self.schema_manager.get_schema(org_id)
            
            # Run checks based on type
            results = {
                "org_id": org_id,
                "check_type": check_type,
                "regulations": regulations,
                "timestamp": datetime.utcnow().isoformat(),
                "summary": {
                    "status": "UNKNOWN",
                    "issues_found": 0,
                    "warnings": 0,
                    "recommendations": 0
                }
            }
            
            if check_type in ["full", "pii"]:
                pii_results = await self._check_pii_compliance(schema, columns)
                results["pii_compliance"] = pii_results
                
            if check_type in ["full", "consent"]:
                consent_results = await self._check_consent_compliance(schema, org_id)
                results["consent_compliance"] = consent_results
                
            if check_type in ["full", "retention"]:
                retention_results = await self._check_retention_compliance(schema)
                results["retention_compliance"] = retention_results
                
            if check_type == "regulations":
                reg_results = await self._check_specific_regulations(
                    schema, regulations, org_id
                )
                results["regulation_compliance"] = reg_results
            
            # Update summary
            self._update_summary(results)
            
            # Generate report if requested
            if generate_report:
                results["compliance_report"] = self._generate_compliance_report(results)
            
            return results
            
        except Exception as e:
            logger.error(f"Compliance check failed: {e}")
            return {
                "error": str(e),
                "org_id": org_id,
                "check_type": check_type
            }
        finally:
            # Cleanup
            await self.catalog_api.disconnect()
            await self.metadata_api.disconnect()
    
    async def _check_pii_compliance(
        self, 
        schema: Dict[str, Any],
        columns: Optional[List[str]]
    ) -> Dict[str, Any]:
        """Check PII compliance."""
        # Get marked PII columns
        marked_pii = []
        detected_pii = self.schema_manager.detect_pii_columns(schema)
        
        # Filter by requested columns if specified
        all_attributes = schema["raw_attributes"]
        if columns:
            all_attributes = [a for a in all_attributes if a["name"] in columns]
        
        # Check each column
        issues = []
        warnings = []
        
        for attr in all_attributes:
            col_name = attr["name"]
            is_marked_pii = attr.get("isRawPII", False)
            
            # Check if PII is properly marked
            if is_marked_pii:
                marked_pii.append(col_name)
            
            # Check if detected but not marked
            col_name_lower = col_name.lower()
            for sensitivity, detected_cols in detected_pii.items():
                if col_name in detected_cols and not is_marked_pii:
                    issues.append({
                        "column": col_name,
                        "issue": "Potential PII not marked",
                        "sensitivity": sensitivity,
                        "recommendation": "Review and mark as PII if confirmed"
                    })
            
            # Check encryption for high sensitivity PII
            if is_marked_pii or col_name in detected_pii["high"]:
                # In real implementation, would check encryption status
                warnings.append({
                    "column": col_name,
                    "warning": "Ensure PII data is encrypted at rest and in transit",
                    "requirement": "GDPR Article 32"
                })
        
        return {
            "marked_pii_columns": len(marked_pii),
            "detected_pii": {
                "high": len(detected_pii["high"]),
                "medium": len(detected_pii["medium"]),
                "low": len(detected_pii["low"])
            },
            "issues": issues,
            "warnings": warnings,
            "recommendations": [
                "Implement data classification system",
                "Regular PII audits recommended",
                "Consider pseudonymization for analytics"
            ]
        }
    
    async def _check_consent_compliance(
        self, 
        schema: Dict[str, Any],
        org_id: str
    ) -> Dict[str, Any]:
        """Check consent management compliance."""
        consent_store = schema["stores"].get("consent_store", [])
        
        # Check for required consent columns
        required_consent_fields = [
            "gdpr_consent",
            "marketing_consent",
            "consent_timestamp",
            "consent_version"
        ]
        
        found_fields = []
        missing_fields = []
        
        consent_column_names = [col["name"].lower() for col in consent_store]
        
        for field in required_consent_fields:
            if any(field in col_name for col_name in consent_column_names):
                found_fields.append(field)
            else:
                missing_fields.append(field)
        
        # Check consent granularity
        consent_types = []
        for col in consent_store:
            if "consent" in col["name"].lower():
                consent_types.append(col["name"])
        
        issues = []
        if not consent_store:
            issues.append({
                "issue": "No consent store found",
                "severity": "HIGH",
                "recommendation": "Implement consent management system"
            })
        
        if missing_fields:
            issues.append({
                "issue": f"Missing consent fields: {', '.join(missing_fields)}",
                "severity": "MEDIUM",
                "recommendation": "Add missing consent tracking fields"
            })
        
        return {
            "has_consent_store": len(consent_store) > 0,
            "consent_fields_found": found_fields,
            "consent_fields_missing": missing_fields,
            "consent_types": consent_types,
            "consent_granularity": "GOOD" if len(consent_types) > 3 else "NEEDS_IMPROVEMENT",
            "issues": issues,
            "recommendations": [
                "Implement consent versioning",
                "Track consent withdrawal",
                "Maintain consent audit trail"
            ]
        }
    
    async def _check_retention_compliance(
        self, 
        schema: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Check data retention compliance."""
        # Look for timestamp columns
        timestamp_columns = []
        for attr in schema["raw_attributes"]:
            if attr.get("dataType") in ["TIMESTAMP", "DATE", "DATETIME"]:
                timestamp_columns.append(attr["name"])
        
        # Check for deletion/retention markers
        retention_markers = []
        for attr in schema["raw_attributes"]:
            col_name = attr["name"].lower()
            if any(marker in col_name for marker in ["deleted", "retention", "expiry", "ttl"]):
                retention_markers.append(attr["name"])
        
        issues = []
        warnings = []
        
        if not timestamp_columns:
            issues.append({
                "issue": "No timestamp columns found",
                "severity": "HIGH",
                "impact": "Cannot determine data age for retention policies"
            })
        
        if not retention_markers:
            warnings.append({
                "warning": "No retention policy markers found",
                "recommendation": "Add retention period tracking"
            })
        
        return {
            "timestamp_columns": timestamp_columns,
            "retention_markers": retention_markers,
            "has_retention_tracking": len(retention_markers) > 0,
            "issues": issues,
            "warnings": warnings,
            "recommendations": [
                "Implement automated data retention policies",
                "Add data lifecycle management",
                "Regular retention policy audits"
            ],
            "retention_periods": {
                "GDPR": {
                    "default": "No longer than necessary",
                    "marketing": "2 years after last interaction",
                    "employee_data": "6 years after employment ends"
                },
                "CCPA": {
                    "default": "12 months unless longer retention justified"
                }
            }
        }
    
    async def _check_specific_regulations(
        self,
        schema: Dict[str, Any],
        regulations: List[str],
        org_id: str
    ) -> Dict[str, Any]:
        """Check compliance for specific regulations."""
        results = {}
        
        for regulation in regulations:
            if regulation.upper() == "GDPR":
                results["GDPR"] = await self._check_gdpr_requirements(schema, org_id)
            elif regulation.upper() == "CCPA":
                results["CCPA"] = await self._check_ccpa_requirements(schema, org_id)
            elif regulation.upper() == "HIPAA":
                results["HIPAA"] = await self._check_hipaa_requirements(schema)
            else:
                results[regulation] = {
                    "status": "NOT_IMPLEMENTED",
                    "message": f"Compliance check for {regulation} not implemented"
                }
        
        return results
    
    async def _check_gdpr_requirements(
        self, 
        schema: Dict[str, Any],
        org_id: str
    ) -> Dict[str, Any]:
        """Check GDPR specific requirements."""
        requirements = {
            "lawful_basis": "UNKNOWN",
            "data_minimization": "UNKNOWN", 
            "purpose_limitation": "UNKNOWN",
            "right_to_erasure": "NOT_IMPLEMENTED",
            "right_to_portability": "PARTIAL",
            "privacy_by_design": "UNKNOWN"
        }
        
        # Check for consent tracking
        consent_store = schema["stores"].get("consent_store", [])
        if consent_store:
            requirements["lawful_basis"] = "IMPLEMENTED"
        
        # Check data minimization
        total_columns = schema["total_columns"]
        pii_columns = len([a for a in schema["raw_attributes"] if a.get("isRawPII", False)])
        if pii_columns / max(total_columns, 1) < 0.2:
            requirements["data_minimization"] = "GOOD"
        else:
            requirements["data_minimization"] = "NEEDS_REVIEW"
        
        return {
            "regulation": "GDPR",
            "status": "PARTIAL_COMPLIANCE",
            "requirements": requirements,
            "action_items": [
                "Implement right to erasure (Article 17)",
                "Document lawful basis for processing",
                "Create data processing registry",
                "Implement data portability APIs"
            ]
        }
    
    async def _check_ccpa_requirements(
        self, 
        schema: Dict[str, Any],
        org_id: str
    ) -> Dict[str, Any]:
        """Check CCPA specific requirements."""
        requirements = {
            "consumer_rights": "UNKNOWN",
            "opt_out_mechanism": "NOT_FOUND",
            "data_deletion": "NOT_IMPLEMENTED",
            "data_disclosure": "PARTIAL"
        }
        
        # Check for opt-out columns
        for attr in schema["raw_attributes"]:
            if "opt_out" in attr["name"].lower() or "do_not_sell" in attr["name"].lower():
                requirements["opt_out_mechanism"] = "FOUND"
                break
        
        return {
            "regulation": "CCPA",
            "status": "PARTIAL_COMPLIANCE",
            "requirements": requirements,
            "action_items": [
                "Implement consumer data deletion process",
                "Add 'Do Not Sell' opt-out mechanism",
                "Create data collection disclosure",
                "Implement consumer request handling"
            ]
        }
    
    async def _check_hipaa_requirements(self, schema: Dict[str, Any]) -> Dict[str, Any]:
        """Check HIPAA specific requirements."""
        # Look for health-related columns
        health_columns = []
        for attr in schema["raw_attributes"]:
            col_name = attr["name"].lower()
            if any(term in col_name for term in ["health", "medical", "diagnosis", "treatment", "medication"]):
                health_columns.append(attr["name"])
        
        if not health_columns:
            return {
                "regulation": "HIPAA",
                "status": "NOT_APPLICABLE",
                "message": "No health-related data detected"
            }
        
        return {
            "regulation": "HIPAA",
            "status": "REQUIRES_REVIEW",
            "health_columns_found": health_columns,
            "requirements": {
                "encryption": "REQUIRED",
                "access_controls": "REQUIRED",
                "audit_trails": "REQUIRED",
                "breach_notification": "REQUIRED"
            },
            "action_items": [
                "Implement HIPAA-compliant encryption",
                "Set up access control lists",
                "Enable comprehensive audit logging",
                "Create breach notification procedures"
            ]
        }
    
    def _update_summary(self, results: Dict[str, Any]):
        """Update compliance summary."""
        total_issues = 0
        total_warnings = 0
        total_recommendations = 0
        
        # Count issues and warnings
        for key, value in results.items():
            if isinstance(value, dict):
                if "issues" in value:
                    total_issues += len(value["issues"])
                if "warnings" in value:
                    total_warnings += len(value["warnings"])
                if "recommendations" in value:
                    total_recommendations += len(value["recommendations"])
        
        # Determine overall status
        if total_issues == 0 and total_warnings == 0:
            status = "COMPLIANT"
        elif total_issues == 0:
            status = "COMPLIANT_WITH_WARNINGS"
        elif total_issues < 5:
            status = "MINOR_ISSUES"
        else:
            status = "MAJOR_ISSUES"
        
        results["summary"] = {
            "status": status,
            "issues_found": total_issues,
            "warnings": total_warnings,
            "recommendations": total_recommendations
        }
    
    def _generate_compliance_report(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate detailed compliance report."""
        report = {
            "executive_summary": f"Compliance check completed with status: {results['summary']['status']}",
            "report_date": results["timestamp"],
            "scope": {
                "organization": results["org_id"],
                "regulations_checked": results["regulations"],
                "check_type": results["check_type"]
            },
            "findings": {
                "critical_issues": [],
                "warnings": [],
                "recommendations": []
            },
            "next_steps": []
        }
        
        # Compile findings
        for key, value in results.items():
            if isinstance(value, dict):
                if "issues" in value:
                    for issue in value["issues"]:
                        report["findings"]["critical_issues"].append({
                            "area": key,
                            "issue": issue
                        })
                if "warnings" in value:
                    for warning in value["warnings"]:
                        report["findings"]["warnings"].append({
                            "area": key,
                            "warning": warning
                        })
                if "recommendations" in value:
                    report["findings"]["recommendations"].extend(value["recommendations"])
        
        # Prioritized next steps
        if report["findings"]["critical_issues"]:
            report["next_steps"].append("Address critical compliance issues immediately")
        report["next_steps"].extend([
            "Implement automated compliance monitoring",
            "Schedule regular compliance audits",
            "Train team on data privacy requirements"
        ])
        
        return report