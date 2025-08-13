"""Tests for compliance checker tool."""

import pytest
from src.tools.compliance import ComplianceCheckerTool


class TestComplianceCheckerTool:
    """Test compliance checker tool functionality."""
    
    @pytest.fixture
    async def compliance_tool(self):
        """Create compliance tool instance."""
        tool = ComplianceCheckerTool()
        return tool
    
    @pytest.mark.asyncio
    async def test_full_compliance_check(self, compliance_tool):
        """Test full compliance check."""
        result = await compliance_tool.run(
            org_id="test_org",
            check_type="full",
            regulations=["GDPR", "CCPA"],
            generate_report=True
        )
        
        assert "org_id" in result
        assert "check_type" in result
        assert result["check_type"] == "full"
        assert "summary" in result
        assert "timestamp" in result
        
        # Check summary structure
        summary = result["summary"]
        assert "status" in summary
        assert "issues_found" in summary
        assert "warnings" in summary
        assert "recommendations" in summary
        
        # Check all compliance areas are covered
        assert "pii_compliance" in result
        assert "consent_compliance" in result
        assert "retention_compliance" in result
        
        # Check report generation
        assert "compliance_report" in result
    
    @pytest.mark.asyncio
    async def test_pii_compliance_check(self, compliance_tool):
        """Test PII compliance check."""
        result = await compliance_tool.run(
            org_id="test_org",
            check_type="pii",
            columns=["email", "phone_number", "user_id", "age"]
        )
        
        assert "pii_compliance" in result
        pii = result["pii_compliance"]
        
        assert "marked_pii_columns" in pii
        assert "detected_pii" in pii
        assert "issues" in pii
        assert "warnings" in pii
        assert "recommendations" in pii
        
        # Check PII detection results
        detected = pii["detected_pii"]
        assert "high" in detected
        assert "medium" in detected
        assert "low" in detected
    
    @pytest.mark.asyncio
    async def test_consent_compliance_check(self, compliance_tool):
        """Test consent compliance check."""
        result = await compliance_tool.run(
            org_id="test_org",
            check_type="consent"
        )
        
        assert "consent_compliance" in result
        consent = result["consent_compliance"]
        
        assert "has_consent_store" in consent
        assert "consent_fields_found" in consent
        assert "consent_fields_missing" in consent
        assert "consent_types" in consent
        assert "consent_granularity" in consent
        assert "issues" in consent
        assert "recommendations" in consent
        
        # Check recommendations
        recs = consent["recommendations"]
        assert any("versioning" in r for r in recs)
        assert any("audit trail" in r for r in recs)
    
    @pytest.mark.asyncio
    async def test_retention_compliance_check(self, compliance_tool):
        """Test retention compliance check."""
        result = await compliance_tool.run(
            org_id="test_org",
            check_type="retention"
        )
        
        assert "retention_compliance" in result
        retention = result["retention_compliance"]
        
        assert "timestamp_columns" in retention
        assert "retention_markers" in retention
        assert "has_retention_tracking" in retention
        assert "issues" in retention
        assert "warnings" in retention
        assert "recommendations" in retention
        assert "retention_periods" in retention
        
        # Check retention periods include regulations
        periods = retention["retention_periods"]
        assert "GDPR" in periods
        assert "CCPA" in periods
    
    @pytest.mark.asyncio
    async def test_gdpr_specific_check(self, compliance_tool):
        """Test GDPR specific compliance check."""
        result = await compliance_tool.run(
            org_id="test_org",
            check_type="regulations",
            regulations=["GDPR"]
        )
        
        assert "regulation_compliance" in result
        assert "GDPR" in result["regulation_compliance"]
        
        gdpr = result["regulation_compliance"]["GDPR"]
        assert "regulation" in gdpr
        assert gdpr["regulation"] == "GDPR"
        assert "status" in gdpr
        assert "requirements" in gdpr
        assert "action_items" in gdpr
        
        # Check GDPR requirements
        reqs = gdpr["requirements"]
        assert "lawful_basis" in reqs
        assert "data_minimization" in reqs
        assert "right_to_erasure" in reqs
        assert "privacy_by_design" in reqs
    
    @pytest.mark.asyncio
    async def test_ccpa_specific_check(self, compliance_tool):
        """Test CCPA specific compliance check."""
        result = await compliance_tool.run(
            org_id="test_org",
            check_type="regulations",
            regulations=["CCPA"]
        )
        
        assert "CCPA" in result["regulation_compliance"]
        
        ccpa = result["regulation_compliance"]["CCPA"]
        assert "consumer_rights" in ccpa["requirements"]
        assert "opt_out_mechanism" in ccpa["requirements"]
        assert "data_deletion" in ccpa["requirements"]
        assert "data_disclosure" in ccpa["requirements"]
    
    @pytest.mark.asyncio
    async def test_hipaa_check(self, compliance_tool):
        """Test HIPAA compliance check."""
        result = await compliance_tool.run(
            org_id="test_org",
            check_type="regulations",
            regulations=["HIPAA"]
        )
        
        assert "HIPAA" in result["regulation_compliance"]
        hipaa = result["regulation_compliance"]["HIPAA"]
        
        # Should detect if health data exists
        assert "status" in hipaa
        if hipaa["status"] == "REQUIRES_REVIEW":
            assert "health_columns_found" in hipaa
            assert "requirements" in hipaa
            assert "encryption" in hipaa["requirements"]
    
    @pytest.mark.asyncio
    async def test_compliance_status_determination(self, compliance_tool):
        """Test overall compliance status determination."""
        result = await compliance_tool.run(
            org_id="test_org",
            check_type="full"
        )
        
        summary = result["summary"]
        status = summary["status"]
        
        # Status should be consistent with findings
        if summary["issues_found"] == 0 and summary["warnings"] == 0:
            assert status == "COMPLIANT"
        elif summary["issues_found"] == 0:
            assert status == "COMPLIANT_WITH_WARNINGS"
        elif summary["issues_found"] < 5:
            assert status == "MINOR_ISSUES"
        else:
            assert status == "MAJOR_ISSUES"
    
    @pytest.mark.asyncio
    async def test_compliance_report_generation(self, compliance_tool):
        """Test compliance report generation."""
        result = await compliance_tool.run(
            org_id="test_org",
            check_type="full",
            generate_report=True
        )
        
        assert "compliance_report" in result
        report = result["compliance_report"]
        
        assert "executive_summary" in report
        assert "report_date" in report
        assert "scope" in report
        assert "findings" in report
        assert "next_steps" in report
        
        # Check findings structure
        findings = report["findings"]
        assert "critical_issues" in findings
        assert "warnings" in findings
        assert "recommendations" in findings
        
        # Check scope
        scope = report["scope"]
        assert "organization" in scope
        assert "regulations_checked" in scope
        assert "check_type" in scope
    
    @pytest.mark.asyncio
    async def test_error_handling(self, compliance_tool):
        """Test error handling in compliance tool."""
        result = await compliance_tool.run(
            org_id="",  # Invalid org_id
            check_type="full"
        )
        
        assert "error" in result
        assert "org_id" in result
    
    @pytest.mark.asyncio
    async def test_specific_columns_check(self, compliance_tool):
        """Test compliance check for specific columns."""
        result = await compliance_tool.run(
            org_id="test_org",
            check_type="pii",
            columns=["email", "ssn", "credit_card"]
        )
        
        # Should only check specified columns
        pii = result["pii_compliance"]
        assert "issues" in pii
        
        # Issues should be related to specified columns
        for issue in pii["issues"]:
            if "column" in issue:
                assert issue["column"] in ["email", "ssn", "credit_card"]