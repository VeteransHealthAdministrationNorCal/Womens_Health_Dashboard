
--------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------
--	Greetings and salutations!																	--
--	This is the Modified Women's Health Dashboard (beta), based on the most excellent work of	--
--	Donna Reagan, R4_VISN2. I've simply reformatted to create a stand-alone product that can,	--
--	hopefully, be easily implemented in any Sta3n for tracking 'Mam & Pap' concerns. Please		--
--	keep me informed of any modifications you make, issues you discover, or additions you'd		--
--	like to see implemented.																	--
--																								--
--	Kindest regards,																			--
--		Harry L. George IV																		--
--		Program Analyst																			--
--		Harry.George2@VA.Gov																	--
--		R1_VISN21, Northern California Health Care System										--
--																								--
--	Implementation instructions:																--
--	1. Set the @Sta3n variable to your Sta3n. For instance, I'm using @Sta3n = 612. You may		--
--		need to make some other slight changes if your station uses Sta6n.						--
--	2. Update table names, depending on the region you are in. For instance, CytoPath and		--
--		SurgPath tables may be named differently.												--
--	3. Set the final table location to a database you have write permissions on and create a	--
--		secured view on LSV for your customers.													--
--	4. Talk to your local CDW/RDW contact to have this stored procedure added to nightly ETL	--
--		run. NOTE: Ensure that there is not another stored procedure by the same name for		--
--		your region, otherwise you will be overwriting the SP being used by another station.	--
--		For instance, my SP is named "MAC.SP_WHD", with the "MAC." prefix denoting my Sta3n.	--
--	5. Decide how you want to report this data. For instance, I have a fairly simple .rdl file	--
--		on FRE, and our WH folks export the data from it as a spreadsheet they make notes on.	--
--------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------


--------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------
--	OVERVIEW:																					--
--		DECLARE VARIABLES																		--
--		BREAST-RELATED																			--
--			DROP TEMP TABLES																	--
--			BUILD ICD TABLES																	--
--			BUILD CPT TABLE																		--
--			BUILD HEALTHFACTOR TABLE															--
--			BUILD CYTOPATHOLOGY TABLE															--
--			BUILD SURGICALPATHOLOGY TABLE														--
--			JOIN SECTION TABLES																	--
--		CERVICAL/OVARIAN/VAGINAL-RELATED														--
--			DROP TEMP TABLES																	--
--			BUILD ICD TABLES																	--
--			BUILD CPT TABLE																		--
--			BUILD HEALTHFACTOR TABLE															--
--			BUILD CYTOPATHOLOGY TABLE															--
--			BUILD SURGICALPATHOLOGY TABLE														--
--			JOIN SECTION TABLES																	--
--		UNION JOINED SECTION-TABLES																--
--		BUILD FINAL (LSV) TABLE																	--
--------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------


--------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------
--	UPDATES:																					--
--																								--
--	2019-04:																					--
--	- Refactored 'search terms' for HealthFactors, ICD (9 & 10), and CPT Codes to catch			--
--		missing items of interest and improve performance.										--
--	- Expanded scope for pregnancy/meternity support.
--	- Added SurgicalPathology tables to catch missing items of interest.						--
--	- Removed patients who have changed their address outside our VISN over two years ago or	--
--		are no longer enrolled or eligible.														--
--	- Split off Hysterectomy/Mastectomy into their own stream using full tables, rather than	--
--		rolling 3-year tables.																	--
--	- Age column changed to reflect age at EventDateTime.										--
--	- Filter to retain only patients where EnrollmentCategory = 'ENROLLED'.						--			
--------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------
DECLARE @Sta3n INT = 612

DROP TABLE IF EXISTS
	#WHD_Mamm_ICD10				-- Get local ICD10 codes
	,#WHD_Mamm_ICD9				-- Get local ICD9 codes
	,#WHD_Mamm_CPT				-- Get all CPT codes used at Sta3n for breast imaging
	,#WHD_Mamm_Rad_CPT			-- Get Radiology codes for exams from #WHD_Mamm_CPT
	,#WHD_Mamm_HealthFactors	-- Get mammograms performed outside of VA
	,#WHD_Mamm_CytoPath			-- Get CytoPathology results for breast exams
	,#WHD_Mamm_SurgPath			-- Get SurgicalPathology results for breast exams
	,#WHD_Mamm					-- Combine Mamm tables

-- Build local Sta3n ICD tables
SELECT
	DimICD10.ICD10Description
	,DimICD10.ICD10SID
	,DimICD10.Sta3n
INTO
	#WHD_Mamm_ICD10
FROM
	CDWWork.Dim.ICD10DescriptionVersion AS DimICD10
WHERE
	DimICD10.Sta3n = @Sta3n
CREATE CLUSTERED INDEX
	CIX_Sta3nICD
		ON #WHD_Mamm_ICD10 (Sta3n, ICD10SID)

SELECT
	DimICD9.ICD9Description
	,DimICD9.ICD9SID
	,DimICD9.Sta3n
INTO
	#WHD_Mamm_ICD9
FROM
	CDWWork.Dim.ICD9DescriptionVersion AS DimICD9
WHERE
	DimICD9.Sta3n = @Sta3n
CREATE CLUSTERED INDEX
	CIX_Sta3nICD
		ON #WHD_Mamm_ICD9 (Sta3n, ICD9SID)

-- Get CPT
SELECT
	VProc.Sta3n
	,VProc.PatientSID
	,VProc.VProcedureSID
	,VProc.VisitSID
	,VProc.VisitDateTime
	,VProc.CPTSID
	,DimCPT.CPTCode
	,DimCPT.CPTName
	,VDiag.ICD10SID
	,VDiag.ICD9SID
	,CAST(NULL AS VARCHAR) AS Abnormal
	,CAST(NULL AS VARCHAR) AS MR
INTO 
	#WHD_Mamm_CPT 
FROM 
	CDWWork.Dim.CPT AS DimCPT
	INNER JOIN CDWWork.Outpat.VProcedure AS VProc
		ON DimCPT.CPTSID = VProc.CPTSID
		AND DimCPT.Sta3n = VProc.Sta3n
	LEFT JOIN CDWWork.Outpat.VDiagnosis AS VDiag
		ON VProc.PatientSID = VDiag.PatientSID
		AND VProc.VisitSID = VDiag.VisitSID
		AND VProc.Sta3n = VDiag.Sta3n
WHERE 
	DimCPT.Sta3n = @Sta3n
	AND	(
		DimCPT.CPTCategory = 'BREAST MAMMOGRAPHY'
		OR (
			(
				DimCPT.CPTCategory = '*Missing*'
				OR DimCPT.CPTCategory = 'CMS HOSPITAL OUTPATIENT PAYMENT SYSTEM'
				OR DimCPT.CPTCategory = 'DIAGNOSTIC RADIOLOGY (DIAGNOSTIC IMAGING)'
				OR DimCPT.CPTCategory = 'DIAGNOSTIC RADIOLOGY (DIAGNOSTIC IMAGING) MISCELLANEOUS'
				OR DimCPT.CPTCategory = 'DIAGNOSTIC SCREENING PROCESSES OR RESULTS'
				OR DimCPT.CPTCategory = 'DIAGNOSTIC ULTRASOUND'
				OR DimCPT.CPTCategory = 'FOLLOW-UP OR OTHER OUTCOMES'
				OR DimCPT.CPTCategory = 'INTEGUMENTARY SYSTEM'
				OR DimCPT.CPTCategory = 'PATIENT'
			) AND (
				DimCPT.CPTName LIKE '%BREAST%'
				OR DimCPT.CPTName LIKE '%MAMMO%'
				OR DimCPT.CPTName LIKE '%MAST%'
			)
		)
	)
CREATE CLUSTERED INDEX
	CIX_Sta3nCPT
		ON #WHD_Mamm_CPT (Sta3n, PatientSID)

-- Get ICD and BIRADS
SELECT
	Rad.Sta3n
	,#WHD_Mamm_CPT.PatientSID
	,#WHD_Mamm_CPT.VisitDateTime
	,('CPT ' + #WHD_Mamm_CPT.CPTName) AS ProcedureName
	,RadiologyDiagnosticCode AS Diagnosis
	,('ICD10 ' + #WHD_Mamm_ICD10.ICD10Description) AS Code1
	,('ICD9 ' + #WHD_Mamm_ICD9.ICD9Description) AS Code2
	,CASE 
		WHEN RadiologyDiagnosticCode IN (
			'BI-RADS CATEGORY 0' -- Mammo
			,'BI-RADS CATEGORY 3'
			,'BI-RADS CATEGORY 4'
			,'BI-RADS CATEGORY 5'
			,'BI-RADS CATEGORY 6'
			,'MINOR ABNORMALITY' -- Ultrasound
			,'ABNORMALITY, ATTN. NEEDED'
			,'MAJOR ABNORMALITY, NO ATTN. NEEDED'
			,'MAJOR ABNORMALITY, PHYSICIAN AWARE'
			,'POSSIBLE MALIGNANCY, FOLLOW-UP NEEDED'
			,'UNSATISFACTORY/INCOMPLETE EXAM'
			,'Admin see case completion' -- Worth following up on
			,'Outside Report'
		) THEN 'Y' 
		ELSE 'N'
	END AS Abnormal
	,ROW_NUMBER() OVER (PARTITION BY #WHD_Mamm_CPT.PatientSID ORDER BY #WHD_Mamm_CPT.VisitDateTime DESC) AS MR 
INTO
	#WHD_Mamm_Rad_CPT
FROM
	#WHD_Mamm_CPT
	LEFT JOIN #WHD_Mamm_ICD10
		ON #WHD_Mamm_CPT.Sta3n = #WHD_Mamm_ICD10.Sta3n
		AND #WHD_Mamm_CPT.ICD10SID = #WHD_Mamm_ICD10.ICD10SID
	LEFT JOIN #WHD_Mamm_ICD9
		ON #WHD_Mamm_CPT.Sta3n = #WHD_Mamm_ICD9.Sta3n
		AND #WHD_Mamm_CPT.ICD9SID = #WHD_Mamm_ICD9.ICD9SID
	INNER JOIN CDWWork.Rad.RadiologyExam AS Rad
		ON #WHD_Mamm_CPT.VisitSID = Rad.VisitSID
		AND #WHD_Mamm_CPT.Sta3n = Rad.Sta3n
	JOIN CDWWork.Rad.RadiologyRegisteredExam AS Exam
		ON Exam.RadiologyRegisteredExamSID = Rad.RadiologyRegisteredExamSID
		AND Exam.Sta3n = Rad.Sta3n
	JOIN CDWWork.Dim.RadiologyDiagnosticCode AS DiagCode
		ON DiagCode.RadiologyDiagnosticCodeSID = Rad.RadiologyDiagnosticCodeSID
		AND DiagCode.Sta3n = Rad.Sta3n
WHERE
	Rad.Sta3n = @Sta3n
	AND Exam.ExamDateTime > CAST(DATEADD(YEAR, -3, GETDATE()) AS DATETIME2)
	AND Rad.RadiologyHoldCancelReasonSID <= 0 
CREATE CLUSTERED INDEX
	CIX_Sta3nVisit
		ON #WHD_Mamm_Rad_CPT (Sta3n, PatientSID)

-- Get HealthFactors
SELECT
	HF.Sta3n
	,HF.PatientSID
	,HF.HealthFactorDateTime AS VisitDateTime
	,CASE
		WHEN HFType.HealthFactorType IS NOT NULL THEN ('HFT ' + HFType.HealthFactorType) 
		ELSE CAST(NULL  AS VARCHAR)
	END AS ProcedureName
	,HF.Comments AS Diagnosis
	,CASE
		WHEN HFType.HealthFactorCategory IS NOT NULL THEN ('HFC ' + HFType.HealthFactorCategory)
		ELSE CAST(NULL AS VARCHAR)
	END AS Code1
	,CAST(NULL AS VARCHAR) AS Code2
	,CASE 
		WHEN HFType.HealthFactorType IN (
			'HFT WH OUTSIDE ABNL MAMMOGRAM'
			,'HFT WH OUTSIDE INCOMPLETE MAMMOGRAM'
		) THEN 'Y'
		WHEN HFType.HealthFactorType IN (
			'HFT WH BILATERAL MASTECTOMY'
		) THEN 'MASTECTOMY'
	END AS Abnormal
	,ROW_NUMBER() OVER (PARTITION BY HF.PatientSID ORDER BY HF.HealthFactorDateTime DESC) AS MR 
INTO
	#WHD_Mamm_HealthFactors
FROM
	CDWWork.HF.HealthFactor AS HF
	LEFT JOIN CDWWork.Dim.HealthFactorType AS HFType
		ON HF.HealthFactorTypeSID = HFType.HealthFactorTypeSID
WHERE
	HF.Sta3n = @Sta3n
	AND (
		HFType.HealthFactorCategory = 'BREAST CANCER'
		OR HFType.HealthFactorCategory = 'WH MAMMOGRAM'
		OR (
			(
				HFType.HealthFactorCategory = 'CANCER SCREENING'
				OR HFType.HealthFactorCategory = 'OUTSIDE RESULTS'
				OR HFType.HealthFactorCategory = 'REMINDER FACTORS'
			) AND (
				HFType.HealthFactorType LIKE '%MAMMO%'
				OR HFType.HealthFactorType LIKE '%BREAST%'
				OR HFType.HealthFactorType LIKE '%MAST%'
			)
		)
	)
CREATE CLUSTERED INDEX
	CIX_Sta3nVisit
		ON #WHD_Mamm_HealthFactors (Sta3n, PatientSID)

-- Get CytoPathology results
SELECT
	CytoPath.Sta3n
	,CytoPath.PatientSID
	,ISNULL(CytoPath.DateTimeSpecimenTaken, CytoPath.DateTimeSpecimenReceived) AS DateTimeSpecimenTaken
	,(CytoSpec.CollectionSample + ' ' + DimTopo.Topography) AS ProcedureName
	,DimMorph.Morphology
	,('SNOMED ' + DimTopo.SNOMEDCode) AS Code1
	,('Morph ' + DimMorph.SnomedCode) AS Code2
	,CASE 
		WHEN DimMorph.Morphology NOT IN (
			'INSUFFICIENT TISSUE FOR DIAGNOSIS'
			,'NEGATIVE FOR MALIGNANT CELLS'
			,'NO EVIDENCE OF MALIGNANCY'
			,'NORMAL TISSUE MORPHOLOGY'
			,'SUBOPTIMAL SPECIMEN'
			,'UNSATISFACTORY SPECIMEN'
		) THEN 'Y'
		ELSE CAST(NULL AS VARCHAR)
	END AS Abnormal
	,ROW_NUMBER() OVER (PARTITION BY CytoPath.PatientSID, CytoSpec.CollectionSample ORDER BY CytoPath.DateTimeSpecimenTaken DESC) AS MR 
INTO
	#WHD_Mamm_CytoPath
FROM
	CDWWork.BISL_R1VX.R_CytoPathology_CytoPathology AS CytoPath
	LEFT OUTER JOIN CDWWork.BISL_R1VX.R_CytoPathology_OrganTissueMorphology AS Morph
		ON CytoPath.CytopathologyIEN = Morph.CytopathologyIEN
		AND CytoPath.LabDataIEN = Morph.LabDataIEN
		AND CytoPath.Sta3n = Morph.Sta3n
	LEFT OUTER JOIN CDWWork.Dim.Morphology AS DimMorph 
		ON Morph.MorphologyFieldIEN = DimMorph.MorphologyIEN 
		AND Morph.Sta3n = DimMorph.Sta3n
	JOIN CDWWork.BISL_R1VX.R_CytoPathology_Specimen AS CytoSpec 
		ON CytoPath.LabDataIEN = CytoSpec.LabDataIEN
		AND CytoPath.CytopathologyIEN = CytoSpec.CytopathologyIEN
		AND CytoPath.Sta3n = CytoSpec.Sta3n
	JOIN CDWWork.Dim.Topography AS DimTopo 
		ON CytoSpec.TopographyFieldIEN = DimTopo.TopographyIEN 
		AND CytoSpec.Sta3n = DimTopo.Sta3n
WHERE 
	CytoPath.Sta3n = @Sta3n
	AND (
		DimTopo.Topography LIKE '%BREAST%'
	)
CREATE CLUSTERED INDEX
	CIX_Sta3nPatient
		ON #WHD_Mamm_CytoPath (Sta3n, PatientSID)

-- Get SurgicalPathology results
SELECT
	SurgPath.Sta3n
	,SurgPath.PatientSID
	,ISNULL(SurgPath.DateTimeSpecimenTaken, SurgPath.DateTimeSpecimenReceived) AS DateTimeSpecimentTaken
	,(SurgPath.SurgicalPathAccNumber + ' ' + SurgPathSpec.Specimen) AS ProcedureName
	,SurgPathMorph.MorphologyField
	,('SNOMED ' + SurgPathMorphField.SnomedCode) AS Code1
	,CAST(NULL AS VARCHAR) AS Code2
	,CAST(NULL AS VARCHAR) AS Abnormal
	,ROW_NUMBER() OVER (PARTITION BY SurgPath.PatientSID, SurgPathSpec.Specimen ORDER BY SurgPath.DateTimeSpecimenTaken) AS MR
INTO
	#WHD_Mamm_SurgPath
FROM
	CDWWork.BISL_R1VX.R_SurgPathology_SurgicalPathology AS SurgPath
	LEFT JOIN CDWWork.BISL_R1VX.R_SurgPathology_SurgPathSpecimen AS SurgPathSpec
		ON SurgPath.SurgicalPathologyIEN = SurgPathSpec.SurgicalPathologyIEN
		AND SurgPath.Sta3n = SurgPathSpec.Sta3n
		AND SurgPath.LabDataIEN = SurgPathSpec.LabDataIEN
	LEFT JOIN CDWWork.BISL_R1VX.R_SurgPathology_SurgPathMorphology AS SurgPathMorph
		ON SurgPath.SurgicalPathologyIEN = SurgPathMorph.SurgicalPathologyIEN
		AND SurgPath.Sta3n = SurgPathMorph.Sta3n
		AND SurgPath.LabDataIEN = SurgPathMorph.LabDataIEN
	LEFT JOIN CDWWork.BISL_R1VX.R_SurgPathology_MorphologyField AS SurgPathMorphField
		ON SurgPathMorph.MorphologyFieldIEN = SurgPathMorphField.MorphologyFieldIEN
		AND SurgPathMorph.Sta3n = SurgPathMorphField.Sta3n
WHERE
	SurgPath.Sta3n = @Sta3n
	AND (
		SurgPathSpec.Specimen LIKE '%BREAST%'
		OR SurgPathSpec.Specimen LIKE '%MASTECTOMY%'
)
CREATE CLUSTERED INDEX
	CIX_Sta3nVisit
		ON #WHD_Mamm_SurgPath (Sta3n, PatientSID)

-- Combine Mamm tables
SELECT 
	Sta3n
	,PatientSID
	,VisitDateTime
	,ProcedureName
	,Diagnosis
	,Code1
	,Code2
	,Abnormal
	,MR 
INTO
	#WHD_Mamm
FROM
	#WHD_Mamm_Rad_CPT
UNION
SELECT
	Sta3n
	,PatientSID
	,VisitDateTime
	,ProcedureName
	,Diagnosis
	,Code1
	,Code2
	,Abnormal
	,MR
FROM
	#WHD_Mamm_HealthFactors
UNION
SELECT
	Sta3n
	,PatientSID
	,DateTimeSpecimenTaken
	,ProcedureName
	,Morphology
	,Code1
	,Code2
	,Abnormal
	,MR
FROM
	#WHD_Mamm_CytoPath
UNION
SELECT
	Sta3n
	,PatientSID
	,DateTimeSpecimentTaken
	,ProcedureName
	,MorphologyField
	,Code1
	,Code2
	,Abnormal
	,MR
FROM
	#WHD_Mamm_SurgPath

DROP TABLE IF EXISTS
	#WHD_Pap_ICD10				-- Get local ICD10 codes
	,#WHD_Pap_ICD9				-- Get local ICD9 codes
	,#WHD_Pap_CPT				-- Get all CPT codes used at Sta3n for pap smear / HPV
	,#WHD_Pap_HealthFactors		-- Get HealthFactors for outside pap smear
	,#WHD_Pap_HPVTests			-- Get HPV tests and results
	,#WHD_Pap_CytoPath			-- Get CytoPathology results for Cervical/vaginal exams	
	,#WHD_Pap_SurgPath			-- Get SurgicalPathology results for Cervical/vaginal exams	
	,#WHD_Pap					-- Combine Pap tables

-- Build local Sta3n ICD tables
SELECT
	DimICD10.ICD10Description
	,DimICD10.ICD10SID
	,DimICD10.Sta3n
INTO
	#WHD_Pap_ICD10
FROM
	CDWWork.Dim.ICD10DescriptionVersion AS DimICD10
WHERE
	DimICD10.Sta3n = @Sta3n
CREATE CLUSTERED INDEX
	CIX_Sta3nICD
		ON #WHD_Pap_ICD10 (Sta3n, ICD10SID)

SELECT
	DimICD9.ICD9Description
	,DimICD9.ICD9SID
	,DimICD9.Sta3n
INTO
	#WHD_Pap_ICD9
FROM
	CDWWork.Dim.ICD9DescriptionVersion AS DimICD9
WHERE
	DimICD9.Sta3n = @Sta3n
CREATE CLUSTERED INDEX
	CIX_Sta3nICD
		ON #WHD_Pap_ICD9 (Sta3n, ICD9SID)

-- Get CPT
SELECT
	VProc.Sta3n
	,VProc.PatientSID
	,VProc.VisitDateTime
	,('CPT ' + DimCPT.CPTName) AS ProcedureName
	,VProc.Comments
	,('ICD10 ' + #WHD_Pap_ICD10.ICD10Description) AS Code1
	,('ICD9 ' + #WHD_Pap_ICD9.ICD9Description) AS Code2
	,CAST(NULL AS VARCHAR) AS Abnormal
	,ROW_NUMBER() OVER (PARTITION BY VProc.PatientSID ORDER BY VProc.VisitDateTime DESC) AS MR 
INTO 
	#WHD_Pap_CPT
FROM 
	CDWWork.Dim.CPT AS DimCPT
	INNER JOIN CDWWork.Outpat.VProcedure AS VProc
		ON VProc.CPTSID = DimCPT.CPTSID
		AND VProc.Sta3n = DimCPT.Sta3n
	LEFT JOIN CDWWork.Outpat.VDiagnosis AS VDiag
		ON VDiag.PatientSID = VProc.PatientSID
		AND VDiag.VisitSID = VProc.VisitSID
		AND VDiag.Sta3n = VProc.Sta3n
	LEFT JOIN #WHD_Pap_ICD10
		ON VDiag.ICD10SID = #WHD_Pap_ICD10.ICD10SID
		AND VDiag.Sta3n = #WHD_Pap_ICD10.Sta3n
	LEFT JOIN #WHD_Pap_ICD9
		ON VDiag.ICD9SID = #WHD_Pap_ICD9.ICD9SID
		AND VDiag.Sta3n = #WHD_Pap_ICD9.Sta3n
WHERE 
	DimCPT.sta3n = @Sta3n
	AND	(
		DimCPT.CPTCategory = 'DIAGNOSTIC RADIOLOGY GYNECOLOGICAL AND OBSTETRICAL'
		OR DimCPT.CPTCategory = 'FEMALE GENITAL SYSTEM'
		OR DimCPT.CPTCategory = 'LAPAROSCOPY/PERITONEOSCOPY/HYSTEROSCOPY'
		OR DimCPT.CPTCategory = 'MATERNITY CARE AND DELIVERY'
		OR DimCPT.CPTCategory = 'OBSTETRIC'
		OR (
			(
				DimCPT.CPTCategory = '*Missing*'
				OR DimCPT.CPTCategory = 'ANATOMIC PATHOLOGY'
				OR DimCPT.CPTCategory = 'CYTOPATHOLOGY'
				OR DimCPT.CPTCategory = 'DIAGNOSTIC RADIOLOGY (DIAGNOSTIC IMAGING)'
				OR DimCPT.CPTCategory = 'DIAGNOSTIC ULTRASOUND'
				OR DimCPT.CPTCategory = 'LOWER ABDOMEN'
				OR DimCPT.CPTCategory = 'PATIENT'
				OR DimCPT.CPTCategory = 'PERINEUM'
				OR DimCPT.CPTCategory = 'URINARY SYSTEM'
			) AND (
				DimCPT.CPTName LIKE '%CERVI%'
				OR DimCPT.CPTName LIKE '%C/V%'
				OR DimCPT.CPTName LIKE '%FETAL%'
				OR DimCPT.CPTName LIKE '%FETUS%'
				OR DimCPT.CPTName LIKE '%GYN%'
				OR DimCPT.CPTName LIKE '%HPV%'
				OR DimCPT.CPTName LIKE '%HYST%'
				OR DimCPT.CPTName LIKE 'OB %'
				OR DimCPT.CPTName LIKE '%PAP%'
				OR DimCPT.CPTName LIKE 'TRANSVAGINAL%'
				OR DimCPT.CPTName LIKE '%UMBILICAL%'
				OR DimCPT.CPTName LIKE '%VAG%'
			)
		)
	)
CREATE CLUSTERED INDEX
	CIX_Sta3nCPT
		ON #WHD_Pap_CPT (Sta3n, PatientSID)

-- Get HealthFactors
SELECT
	HF.Sta3n
	,HF.PatientSID
	,HF.HealthFactorDateTime AS VisitDateTime
	,CASE
		WHEN HFType.HealthFactorType IS NOT NULL THEN ('HFT ' + HFType.HealthFactorType) 
		ELSE CAST(NULL AS VARCHAR)
	END AS ProcedureName
	,HF.Comments AS Diagnosis
	,CASE
		WHEN HFType.HealthFactorCategory IS NOT NULL THEN ('HFC ' + HFType.HealthFactorCategory) 
		ELSE CAST(NULL AS VARCHAR)
	END AS Code1
	,CAST(NULL AS VARCHAR) AS Code2
	,CASE 
		WHEN HFType.HealthFactorType = 'OUTSIDE CERVICAL HPV TESTING POSITIVE' THEN 'HPV POS' 
		WHEN HFType.HealthFactorType = 'WH HYSTERECTOMY W/CERVIX REMOVED' THEN 'HYSTERECTOMY'
		WHEN HFType.HealthFactorType IN (
			'WH OUTSIDE ABNORMAL (ASCUS) PAP'
			,'WH PAP SMEAR SCREEN FREQ - 6M'
		) THEN 'Y'
	END AS Abnormal
	,ROW_NUMBER() OVER (PARTITION BY HF.PatientSID ORDER BY HF.HealthFactorDateTime DESC) AS MR 
INTO
	#WHD_Pap_HealthFactors
FROM
	CDWWork.HF.HealthFactor AS HF
	LEFT JOIN CDWWork.Dim.HealthFactorType AS HFType
		ON HF.HealthFactorTypeSID = HFType.HealthFactorTypeSID
WHERE
	HF.Sta3n = @Sta3n
	AND (
		HFType.HealthFactorCategory = 'PAP RESULTS'
		OR HFType.HealthFactorCategory = 'VA-HPV'
		OR HFType.HealthFactorCategory = 'WH PAP SMEAR'
		OR HFType.HealthFactorCategory = 'HFC MATERNITY CARE'
		OR (
			(
				HFType.HealthFactorCategory = 'OUTSIDE RESULTS'
				OR HFType.HealthFactorCategory = 'PROCEDURES'
				OR HFType.HealthFactorCategory = 'REMINDER FACTORS'
			) AND (
				HFType.HealthFactorType LIKE '%CERVI%'
				OR HFType.HealthFactorType LIKE '%GYN%'
				OR HFType.HealthFactorType LIKE '%HPV%'
				OR HFType.HealthFactorType LIKE '%HYST%'
				OR HFType.HealthFactorType LIKE '%PAP%'
				OR HFType.HealthFactorType LIKE '%VAG%'
			)
		)
	)
CREATE CLUSTERED INDEX
	CIX_Sta3nVisit
		ON #WHD_Pap_HealthFactors (Sta3n, PatientSID)

-- Get HPV and pregnancy test results
SELECT
	LabChem.Sta3n
	,LabChem.PatientSID
	,LabChem.LabChemSpecimenDateTime
	,('LAB ' + DimLabChem.LabChemTestName) AS ProcedureName
	,LabChem.LabChemResultValue
	,CAST(NULL AS VARCHAR) AS Code1
	,CAST(NULL AS VARCHAR) AS Code2
	,CASE 
		WHEN LabChem.LabChemResultValue IN (
			'POS' 
			,'POSITIVE'
		) THEN 'HPV POS' 
	END AS Abnormal
	,ROW_NUMBER() OVER (PARTITION BY LabChem.PatientSID, DimLabChem.LabChemTestName ORDER BY LabChem.LabChemCompleteDateTime DESC) AS MR
INTO
	#WHD_Pap_HPVTests
FROM
	CDWWork.Chem.LabChem AS LabChem
	LEFT JOIN CDWWork.Dim.LabChemTest AS DimLabChem
		ON LabChem.LabChemTestSID = DimLabChem.LabChemTestSID
WHERE
	LabChem.Sta3n = @Sta3n
	AND (
		DimLabChem.LabChemTestName LIKE '%HPV%'
		OR DimLabChem.LabChemTestName LIKE '%PREGNANCY%'
	)
CREATE CLUSTERED INDEX
	CIX_Sta3nPatientSID
		ON #WHD_Pap_HPVTests (Sta3n, PatientSID)

-- Get the CytoPathology results
SELECT 
	CytoPath.Sta3n
	,CytoPath.PatientSID
	,ISNULL(CytoPath.DateTimeSpecimenTaken, CytoPath.DateTimeSpecimenReceived) AS DateTimeSpecimenTaken
	,(CytoSpec.CollectionSample + ' ' + DimTopo.Topography) AS ProcedureName
	,DimMorph.Morphology
	,('SNOMED ' + DimTopo.SNOMEDCode) AS Code1
	,('Morph ' + DimMorph.SnomedCode) AS Code2
	,CASE 
		WHEN DimMorph.Morphology NOT IN (
			'INSUFFICIENT TISSUE FOR DIAGNOSIS'
			,'NEGATIVE FOR MALIGNANT CELLS'
			,'NO EVIDENCE OF MALIGNANCY'
			,'NORMAL TISSUE MORPHOLOGY'
			,'SUBOPTIMAL SPECIMEN'
			,'UNSATISFACTORY SPECIMEN'
		) THEN 'Y'
		ELSE CAST(NULL AS VARCHAR)
	END AS Abnormal
	,ROW_NUMBER() OVER (PARTITION BY CytoPath.PatientSID, CytoSpec.CollectionSample ORDER BY CytoPath.DateTimeSpecimenTaken DESC) AS MR 
INTO 
	#WHD_Pap_CytoPath
FROM
	CDWWork.BISL_R1VX.R_CytoPathology_CytoPathology AS CytoPath
	LEFT OUTER JOIN CDWWork.BISL_R1VX.R_CytoPathology_OrganTissueMorphology AS Morph
		ON CytoPath.CytopathologyIEN = Morph.CytopathologyIEN
		AND CytoPath.LabDataIEN = Morph.LabDataIEN
		AND CytoPath.Sta3n = Morph.Sta3n
	LEFT OUTER JOIN CDWWork.Dim.Morphology AS DimMorph 
		ON Morph.MorphologyFieldIEN = DimMorph.MorphologyIEN 
		AND Morph.Sta3n = DimMorph.Sta3n
	JOIN CDWWork.BISL_R1VX.R_CytoPathology_Specimen AS CytoSpec 
		ON CytoPath.LabDataIEN = CytoSpec.LabDataIEN
		AND CytoPath.CytopathologyIEN = CytoSpec.CytopathologyIEN
		AND CytoPath.Sta3n = CytoSpec.Sta3n
	JOIN CDWWork.Dim.Topography AS DimTopo 
		ON CytoSpec.TopographyFieldIEN = DimTopo.TopographyIEN 
		AND CytoSpec.Sta3n = DimTopo.Sta3n
WHERE 
	CytoPath.Sta3n = @Sta3n
	AND (
		DimTopo.Topography = 'CERVIX'
		OR DimTopo.Topography LIKE 'CERVICAL CYTOLOGIC MATERIAL'
		OR DimTopo.Topography LIKE 'FEMALE BREAST%'
		OR DimTopo.Topography LIKE '%OVARY'
		OR DimTopo.Topography = 'PELVIC CAVITY'
		OR DimTopo.Topography = 'PERINEUM'
		OR DimTopo.Topography = 'VAGINA'
		OR DimTopo.Topography = 'VAGINA AND CERVIX, CS'
	)
CREATE CLUSTERED INDEX
	CIX_Sta3nPatient
		ON #WHD_Pap_CytoPath (Sta3n, PatientSID)

-- Get Cervical/Vaginal Surgical Pathology results
SELECT
	SurgPath.Sta3n
	,SurgPath.PatientSID
	,ISNULL(SurgPath.DateTimeSpecimenTaken, SurgPath.DateTimeSpecimenReceived) AS DateTimeSpecimentTaken
	,(SurgPath.SurgicalPathAccNumber + ' ' + SurgPathSpec.Specimen) AS ProcedureName
	,SurgPathMorph.MorphologyField
	,('SNOMED ' + SurgPathMorphField.SnomedCode) AS Code1
	,CAST(NULL AS VARCHAR) AS Code2
	,CAST(NULL AS VARCHAR) AS Abnormal
	,ROW_NUMBER() OVER (PARTITION BY SurgPath.PatientSID, SurgPathSpec.Specimen ORDER BY SurgPath.DateTimeSpecimenTaken) AS MR
INTO
	#WHD_Pap_SurgPath
FROM
	CDWWork.BISL_R1VX.R_SurgPathology_SurgicalPathology AS SurgPath
	LEFT JOIN CDWWork.BISL_R1VX.R_SurgPathology_SurgPathSpecimen AS SurgPathSpec
		ON SurgPath.SurgicalPathologyIEN = SurgPathSpec.SurgicalPathologyIEN
		AND SurgPath.Sta3n = SurgPathSpec.Sta3n
		AND SurgPath.LabDataIEN = SurgPathSpec.LabDataIEN
	LEFT JOIN CDWWork.BISL_R1VX.R_SurgPathology_SurgPathMorphology AS SurgPathMorph
		ON SurgPath.SurgicalPathologyIEN = SurgPathMorph.SurgicalPathologyIEN
		AND SurgPath.Sta3n = SurgPathMorph.Sta3n
		AND SurgPath.LabDataIEN = SurgPathMorph.LabDataIEN
	LEFT JOIN CDWWork.BISL_R1VX.R_SurgPathology_MorphologyField AS SurgPathMorphField
		ON SurgPathMorph.MorphologyFieldIEN = SurgPathMorphField.MorphologyFieldIEN
		AND SurgPathMorph.Sta3n = SurgPathMorphField.Sta3n
WHERE
	SurgPath.Sta3n = @Sta3n
	AND (
		SurgPathSpec.Specimen LIKE '%CERVIX%'
		OR SurgPathSpec.Specimen LIKE '%CERVICAL%'
		OR SurgPathSpec.Specimen LIKE '%VAGINA%'
		OR SurgPathSpec.Specimen LIKE '%OVARY%'
		OR SurgPathSpec.Specimen LIKE '%OVARIAN%'
		OR SurgPathSpec.Specimen LIKE '%HYSTERECTOMY%'
)

-- Combine Pap tables
SELECT
	Sta3n
	,PatientSID
	,VisitDateTime
	,ProcedureName
	,Diagnosis
	,Code1
	,Code2
	,Abnormal
	,MR 
INTO
	#WHD_Pap
FROM
	#WHD_Pap_HealthFactors
UNION
SELECT
	Sta3n
	,PatientSID
	,VisitDateTime
	,ProcedureName
	,Comments
	,Code1
	,Code2
	,Abnormal
	,MR 
FROM
	#WHD_Pap_CPT
UNION
SELECT
	Sta3n
	,PatientSID
	,LabChemSpecimenDateTime
	,ProcedureName
	,LabChemResultValue
	,Code1
	,Code2
	,Abnormal
	,MR
FROM
	#WHD_Pap_HPVTests
UNION
SELECT
	Sta3n
	,PatientSID
	,DateTimeSpecimenTaken
	,ProcedureName
	,Morphology
	,Code1
	,Code2
	,Abnormal
	,MR
FROM
	#WHD_Pap_CytoPath
UNION
SELECT
	Sta3n
	,PatientSID
	,DateTimeSpecimentTaken
	,ProcedureName
	,MorphologyField
	,Code1
	,Code2
	,Abnormal
	,MR
FROM
	#WHD_Pap_SurgPath

DROP TABLE IF EXISTS #WHD

SELECT
	Sta3n
	,PatientSID
	,VisitDateTime
	,ProcedureName
	,Diagnosis
	,Code1
	,Code2
	,Abnormal
	,MR
INTO
	#WHD
FROM
	#WHD_Mamm
UNION
SELECT
	Sta3n
	,PatientSID
	,VisitDateTime
	,ProcedureName
	,Diagnosis
	,Code1
	,Code2
	,Abnormal
	,MR
FROM
	#WHD_Pap
CREATE CLUSTERED COLUMNSTORE INDEX
	WHD_CCI
		ON #WHD

DROP TABLE IF EXISTS D05_VISN21Sites.LSV.MAC_WomensHealthDashboard
SELECT DISTINCT
	#WHD.Sta3n
	,SPatient.PatientName
	,SPatient.PatientSSN
	,(SPatient.Age - DATEDIFF(YY, #WHD.VisitDateTime, GETDATE())) AS AgeAtEvent
	,CAST(#WHD.VisitDateTime AS DATE) AS EventDate
	,#WHD.ProcedureName
	,#WHD.Diagnosis
	,#WHD.Code1
	,#WHD.Code2
	,#WHD.Abnormal
--	,#WHD.MR AS ProcedureRank
	--,ROW_NUMBER() OVER (PARTITION BY SPatient.PatientName ORDER BY MR) AS PatientRank
INTO
	D05_VISN21Sites.LSV.MAC_WomensHealthDashboard
FROM
	#WHD
	INNER JOIN CDWWork.BISL_R1VX_SPatient.AR3Y_SPatient_SPatient AS SPatient
		ON #WHD.PatientSID = SPatient.PatientSID
		AND #WHD.Sta3n = SPatient.Sta3n
	INNER JOIN CDWWork.Patient.Enrollment AS PatientEnrollment
		ON SPatient.PatientSID = PatientEnrollment.PatientSID
		AND SPatient.Sta3n = PatientEnrollment.Sta3n
	INNER JOIN CDWWork.Dim.EnrollmentStatus AS DimEnrollmentStatus
		ON PatientEnrollment.EnrollmentStatusSID = DimEnrollmentStatus.EnrollmentStatusSID
		AND PatientEnrollment.Sta3n = DimEnrollmentStatus.Sta3n
WHERE
	SPatient.PatientName NOT LIKE 'ZZ%'
	AND SPatient.PatientName IS NOT NULL
	AND SPatient.Gender = 'F'
	AND SPatient.DeceasedFlag != 'Y'
	AND DimEnrollmentStatus.EnrollmentCategory = 'ENROLLED'
	AND ProcedureName NOT LIKE '%ORDER%'
--	AND #WHD.MR = 1
ORDER BY
	PatientName
	,EventDate DESC
CREATE CLUSTERED COLUMNSTORE INDEX
	CCSI_MAC_WomensHealthDashboard
		ON D05_VISN21Sites.LSV.MAC_WomensHealthDashboard

SELECT DISTINCT * FROM D05_VISN21Sites.LSV.MAC_WomensHealthDashboard