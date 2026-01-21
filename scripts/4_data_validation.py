import pandas as pd
import great_expectations as gx
from fpdf import FPDF
import datetime
import os
import glob
import ast

# ==========================================
# 1. PATH CONFIGURATION
# ==========================================
BRONZE_BASE = os.path.join("recomart-data-lake", "bronze")
SILVER_BASE = os.path.join("recomart-data-lake", "silver")
os.makedirs(SILVER_BASE, exist_ok=True)

def get_latest_batch(dataset_name, filename):
    search_path = os.path.join(BRONZE_BASE, dataset_name, "dt_*")
    folders = glob.glob(search_path)
    if not folders:
        raise FileNotFoundError(f"No batches found for {dataset_name}")
    latest_folder = max(folders)
    return latest_folder, os.path.basename(latest_folder)

# ==========================================
# 2. ENHANCED AUDIT REPORT GENERATOR
# ==========================================
class RecomartAuditReport(FPDF):
    def header(self):
        self.set_font("Helvetica", "B", 16)
        self.set_text_color(44, 62, 80)
        self.cell(0, 10, "ADVANCED DATA QUALITY AUDIT", ln=True, align="C")
        self.set_font("Helvetica", "I", 9)
        self.cell(0, 5, f"Audit Execution Time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", ln=True, align="C")
        self.ln(10)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 8)
        self.cell(0, 10, f"Page {self.page_no()}", align="C")

    def create_summary_box(self, inter_success, prod_success):
        self.set_font("Helvetica", "B", 12)
        self.cell(0, 10, "Executive Health Summary", ln=True)
        self.set_fill_color(245, 245, 245)
        
        status_data = [("Interactions", inter_success), ("Products", prod_success)]
        for name, success in status_data:
            status = "PASSED" if success else "FAILED"
            self.set_text_color(44, 62, 80)
            self.cell(90, 10, f" {name} Data Integrity:", border=1, fill=True)
            self.set_text_color(0, 128, 0) if success else self.set_text_color(200, 0, 0)
            self.cell(40, 10, f" {status}", border=1, ln=True, fill=True)
        self.set_text_color(44, 62, 80)
        self.ln(5)

    def add_dq_table(self, title, results):
        self.set_font("Helvetica", "B", 11)
        self.cell(0, 10, f"Detailed Constraints: {title}", ln=True)
        self.set_font("Helvetica", "B", 8)
        self.set_fill_color(52, 73, 94)
        self.set_text_color(255, 255, 255)
        self.cell(85, 8, " Expectation Rule", border=1, fill=True)
        self.cell(35, 8, " Target Column", border=1, fill=True)
        self.cell(30, 8, " Status", border=1, fill=True)
        self.cell(40, 8, " Failed Records", border=1, ln=True, fill=True)
        
        self.set_font("Helvetica", "", 7)
        self.set_text_color(0, 0, 0)
        for r in results.results:
            status = "PASS" if r.success else "FAIL"
            color = (0, 128, 0) if r.success else (200, 0, 0)
            self.cell(85, 8, f" {r.expectation_config.type.replace('expect_', '')}", border=1)
            self.cell(35, 8, f" {r.expectation_config.kwargs.get('column', 'Table')}", border=1)
            self.set_text_color(*color)
            self.cell(30, 8, f" {status}", border=1)
            self.set_text_color(0, 0, 0)
            unexpected = r.result.get('unexpected_count', 0) if not r.success else 0
            self.cell(40, 8, f" {unexpected}", border=1, ln=True)
        self.ln(5)

# ==========================================
# 3. MAIN PIPELINE
# ==========================================
def run_advanced_validation():
    try:
        # 1. Load Data
        inter_folder, inter_batch = get_latest_batch("user_interactions", "interactions.csv")
        prod_folder, prod_batch = get_latest_batch("product_catalog", "products.csv")
        df_inter = pd.read_csv(os.path.join(inter_folder, "interactions.csv"))
        df_prod = pd.read_csv(os.path.join(prod_folder, "products.csv"))

        # Pre-processing Rating Dictionary (Extracting numerical rate for validation)
        df_prod['val_rate'] = df_prod['rating'].apply(lambda x: ast.literal_eval(x).get('rate') if isinstance(x, str) else None)
        
        # 2. GX Setup (Using Fluent API)
        context = gx.get_context()
        ds = context.data_sources.add_or_update_pandas(name="recomart_ds")
        
        # Define Interaction Assets
        asset_inter = ds.add_dataframe_asset("inter")
        bd_inter = asset_inter.add_batch_definition_whole_dataframe("bd_inter")
        
        # Define Product Assets
        asset_prod = ds.add_dataframe_asset("prod")
        bd_prod = asset_prod.add_batch_definition_whole_dataframe("bd_prod")

        # 3. ADVANCED EXPECTATIONS: Interactions
        suite_inter = context.suites.add_or_update(gx.ExpectationSuite(name="s_inter"))
        suite_inter.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="user_id"))
        suite_inter.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="interaction_id"))
        suite_inter.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
            column="event_type", value_set=["view", "click", "add_to_cart", "purchase"]
        ))

        # 4. ADVANCED EXPECTATIONS: Product Catalog
        suite_prod = context.suites.add_or_update(gx.ExpectationSuite(name="s_prod"))
        suite_prod.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="id"))
        suite_prod.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="category"))
        suite_prod.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="price", min_value=0.01))
        suite_prod.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="val_rate", min_value=1.0, max_value=5.0))

        # 5. Validation Execution (Strictly using Keyword Arguments to avoid Pydantic errors)
        v_def_inter = context.validation_definitions.add_or_update(
            gx.ValidationDefinition(name="v_inter", data=bd_inter, suite=suite_inter)
        )
        v_def_prod = context.validation_definitions.add_or_update(
            gx.ValidationDefinition(name="v_prod", data=bd_prod, suite=suite_prod)
        )
        
        res_inter = v_def_inter.run(batch_parameters={"dataframe": df_inter})
        res_prod = v_def_prod.run(batch_parameters={"dataframe": df_prod})

        # 6. Promotion & Report
        if res_inter.success and res_prod.success:
            print("Quality Checks Passed. Promoting to Silver Layer...")
            # Interaction Silver Export
            path_i = os.path.join(SILVER_BASE, "user_interactions", inter_batch)
            os.makedirs(path_i, exist_ok=True)
            df_inter.to_csv(os.path.join(path_i, "user_interactions_silver.csv"), index=False)
            
            # Product Silver Export (Dropping the validation helper column)
            path_p = os.path.join(SILVER_BASE, "product_catalog", prod_batch)
            os.makedirs(path_p, exist_ok=True)
            df_prod.drop(columns=['val_rate']).to_csv(os.path.join(path_p, "product_catalog_silver.csv"), index=False)
        else:
            print("Validation Failed. Silver files were not updated.")

        # 7. Generate Enhanced PDF Report
        pdf = RecomartAuditReport()
        pdf.add_page()
        pdf.create_summary_box(res_inter.success, res_prod.success)
        pdf.add_dq_table("User Interactions", res_inter)
        pdf.add_dq_table("Product Catalog", res_prod)
        
        report_name = f"Advanced_DQ_Report_{datetime.datetime.now().strftime('%Y%m%d')}.pdf"
        pdf.output(report_name)
        print(f"Audit Complete. PDF Saved as: {report_name}")

    except Exception as e:
        print(f"Pipeline Error: {e}")

if __name__ == "__main__":
    run_advanced_validation()