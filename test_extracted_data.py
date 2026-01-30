#!/usr/bin/env python3
"""
Test script to fetch and analyze extracted objects and relationships for an assessment.
Uses pandas for data analysis and reporting.
"""
import sys
import uuid
from pathlib import Path
import pandas as pd
from sqlalchemy.orm import Session
from dotenv import load_dotenv

# Load environment variables from .env file
backend_dir = Path(__file__).parent
env_file = backend_dir.parent / ".env"
if env_file.exists():
    load_dotenv(env_file)
else:
    # Try loading from backend directory
    env_file = backend_dir / ".env"
    if env_file.exists():
        load_dotenv(env_file)

# Add backend to path
sys.path.insert(0, str(backend_dir))

from app.db.session import SessionLocal
from app.models.object import ExtractedObject, ObjectRelationship
from app.models.assessment import Assessment


def get_extracted_data(assessment_id: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Fetch all extracted objects and relationships for a given assessment_id.
    
    Returns:
        tuple: (objects_df, relationships_df) - Two pandas DataFrames
    """
    try:
        db: Session = SessionLocal()
    except Exception as e:
        raise ConnectionError(
            f"Failed to connect to database. Please ensure:\n"
            f"  1. Database is running and accessible\n"
            f"  2. DATABASE_URL in .env file is correct\n"
            f"  3. If using Docker, ensure you're running from within the container or update DATABASE_URL\n"
            f"Original error: {e}"
        ) from e
    
    try:
        # Convert string to UUID
        assessment_uuid = uuid.UUID(assessment_id)
        
        # Verify assessment exists
        assessment = db.query(Assessment).filter(Assessment.id == assessment_uuid).first()
        if not assessment:
            raise ValueError(f"Assessment with ID {assessment_id} not found")
        
        print(f"📊 Assessment: {assessment.name} (Status: {assessment.status})")
        print(f"   BI Tool: {assessment.bi_tool}")
        print(f"   Created: {assessment.created_at}")
        print("=" * 80)
        
        # Fetch all objects
        objects = db.query(ExtractedObject).filter(
            ExtractedObject.assessment_id == assessment_uuid
        ).all()
        
        # Fetch all relationships
        relationships = db.query(ObjectRelationship).filter(
            ObjectRelationship.assessment_id == assessment_uuid
        ).all()
        
        print(f"✅ Fetched {len(objects)} objects and {len(relationships)} relationships")
        
        # Convert objects to DataFrame
        objects_data = []
        for obj in objects:
            objects_data.append({
                'id': str(obj.id),
                'object_type': obj.object_type,
                'name': obj.name,
                'path': obj.path,
                'file_id': str(obj.file_id),
                'properties': obj.properties,
                'complexity_score_looker': obj.complexity_score_looker,
                'complexity_level_looker': obj.complexity_level_looker,
                'complexity_score_custom': obj.complexity_score_custom,
                'complexity_level_custom': obj.complexity_level_custom,
                'hierarchy_depth': obj.hierarchy_depth,
                'hierarchy_level': obj.hierarchy_level,
                'hierarchy_path': obj.hierarchy_path,
                'created_at': obj.created_at,
            })
        
        objects_df = pd.DataFrame(objects_data)
        
        # Convert relationships to DataFrame
        relationships_data = []
        for rel in relationships:
            relationships_data.append({
                'id': str(rel.id),
                'source_object_id': str(rel.source_object_id),
                'target_object_id': str(rel.target_object_id),
                'relationship_type': rel.relationship_type,
                'details': rel.details,
                'complexity_score': rel.complexity_score,
                'complexity_level': rel.complexity_level,
                'created_at': rel.created_at,
            })
        
        relationships_df = pd.DataFrame(relationships_data)
        
        # Join with object names for better readability
        if not objects_df.empty and not relationships_df.empty:
            # Create lookup dictionaries
            obj_id_to_name = dict(zip(objects_df['id'], objects_df['name']))
            obj_id_to_type = dict(zip(objects_df['id'], objects_df['object_type']))
            
            # Add source and target names
            relationships_df['source_name'] = relationships_df['source_object_id'].map(obj_id_to_name)
            relationships_df['target_name'] = relationships_df['target_object_id'].map(obj_id_to_name)
            relationships_df['source_type'] = relationships_df['source_object_id'].map(obj_id_to_type)
            relationships_df['target_type'] = relationships_df['target_object_id'].map(obj_id_to_type)
        
        return objects_df, relationships_df
        
    finally:
        db.close()


def analyze_data(objects_df: pd.DataFrame, relationships_df: pd.DataFrame):
    """
    Analyze and display statistics about the extracted data.
    """
    print("\n" + "=" * 80)
    print("📈 DATA ANALYSIS")
    print("=" * 80)
    
    # Objects Analysis
    print("\n📦 EXTRACTED OBJECTS SUMMARY:")
    print("-" * 80)
    print(f"Total Objects: {len(objects_df)}")
    
    # if not objects_df.empty:
    #     print(f"\nObject Types Distribution:")
    #     type_counts = objects_df['object_type'].value_counts()
    #     for obj_type, count in type_counts.items():
    #         percentage = (count / len(objects_df)) * 100
    #         print(f"  {obj_type:30s}: {count:5d} ({percentage:5.1f}%)")
        
    #     print(f"\nObjects with Complexity Scores:")
    #     print(f"  Looker Complexity: {objects_df['complexity_score_looker'].notna().sum()} objects")
    #     print(f"  Custom Complexity: {objects_df['complexity_score_custom'].notna().sum()} objects")
        
    #     print(f"\nHierarchy Information:")
    #     print(f"  Objects with hierarchy_depth: {objects_df['hierarchy_depth'].notna().sum()}")
    #     print(f"  Objects with hierarchy_level: {objects_df['hierarchy_level'].notna().sum()}")
        
    #     if objects_df['complexity_score_looker'].notna().any():
    #         print(f"\nLooker Complexity Score Statistics:")
    #         print(f"  Mean: {objects_df['complexity_score_looker'].mean():.2f}")
    #         print(f"  Min:  {objects_df['complexity_score_looker'].min():.2f}")
    #         print(f"  Max:  {objects_df['complexity_score_looker'].max():.2f}")
    #         print(f"  Std:  {objects_df['complexity_score_looker'].std():.2f}")
    
    # Build and print a "complete tree" of all objects by parent-child ("contains") relationships
    print("\n🌳 OBJECT HIERARCHY TREE (based on 'contains' relationships):")
    print("-" * 80)

    if not objects_df.empty and not relationships_df.empty:
        # Build mapping: parent_id => list of child rows (objects df rows)
        from collections import defaultdict, deque

        # Only use relationships of type 'contains'
        contains_rels = relationships_df[relationships_df['relationship_type'] == 'contains']

        parent_to_children = defaultdict(list)
        child_to_parent = {}

        for _, row in contains_rels.iterrows():
            parent_id = row['source_object_id']
            child_id = row['target_object_id']
            parent_to_children[parent_id].append(child_id)
            child_to_parent[child_id] = parent_id

        # Find all root objects (objects that are never a target in a 'contains' rel)
        all_object_ids = set(objects_df['id'])
        child_ids = set(contains_rels['target_object_id'])
        root_ids = list(all_object_ids - child_ids)
        # Optionally, also consider "top-level" objects by object_type
        # Eg: dashboards, data_sources, etc.
        # root_ids = [obj_id for obj_id in all_object_ids if obj_id not in child_ids or obj_id in (objects_df[objects_df['object_type'].isin(['dashboard','data_source'])]['id'])]

        # Function to walk and print the tree
        def print_tree(obj_id, prefix=""):
            obj_row = objects_df[objects_df['id'] == obj_id]
            if obj_row.empty:
                return
            obj = obj_row.iloc[0]
            obj_name = str(obj['name'])
            obj_type = str(obj['object_type'])
            print(f"{prefix}- [{obj_type}] {obj_name}")
            # Recurse for children
            for child_id in parent_to_children.get(obj_id, []):
                print_tree(child_id, prefix + "    ")

        printed_any = False
        for root_id in root_ids:
            print_tree(root_id)
            printed_any = True

        if not printed_any:
            print("  (No hierarchy found. Check if 'contains' relationships exist in this extraction.)")
    else:
        print("  (Objects or relationships DataFrame is empty. Skipping tree build.)")

    # Relationships Analysis
    print("\n🔗 RELATIONSHIPS SUMMARY:")
    print("-" * 80)
    print(f"Total Relationships: {len(relationships_df)}")
    
    if not relationships_df.empty:
        print(f"\nRelationship Types Distribution:")
        rel_type_counts = relationships_df['relationship_type'].value_counts()
        for rel_type, count in rel_type_counts.items():
            percentage = (count / len(relationships_df)) * 100
            print(f"  {rel_type:30s}: {count:5d} ({percentage:5.1f}%)")
        
        print(f"\nRelationships with Complexity Scores:")
        print(f"  With complexity_score: {relationships_df['complexity_score'].notna().sum()}")
        
        # Top objects by number of outgoing relationships
        if 'source_object_id' in relationships_df.columns:
            outgoing_counts = relationships_df['source_object_id'].value_counts()
            print(f"\nTop 10 Objects by Outgoing Relationships:")
            for obj_id, count in outgoing_counts.head(10).items():
                obj_name = relationships_df[relationships_df['source_object_id'] == obj_id]['source_name'].iloc[0] if 'source_name' in relationships_df.columns else 'N/A'
                print(f"  {obj_name[:50]:50s}: {count} relationships")
        
        # Top objects by number of incoming relationships
        if 'target_object_id' in relationships_df.columns:
            incoming_counts = relationships_df['target_object_id'].value_counts()
            print(f"\nTop 10 Objects by Incoming Relationships:")
            for obj_id, count in incoming_counts.head(10).items():
                obj_name = relationships_df[relationships_df['target_object_id'] == obj_id]['target_name'].iloc[0] if 'target_name' in relationships_df.columns else 'N/A'
                print(f"  {obj_name[:50]:50s}: {count} relationships")
    
    print("\n" + "=" * 80)


def main():
    """Main function to run the analysis."""
    # Assessment ID from user
    assessment_id = "dfe81637-7f9b-491c-97ec-b84308413f4a"
    
    print("🚀 Starting Data Extraction and Analysis")
    print("=" * 80)
    print(f"Assessment ID: {assessment_id}")
    print()
    
    try:
        # Fetch data
        objects_df, relationships_df = get_extracted_data(assessment_id)
        # Print all datasources and their nested details

        if not objects_df.empty:
            ds_mask = objects_df['object_type'].str.lower() == 'data_source'
            data_sources = objects_df[ds_mask].copy()
            print("\n🔎 DATA SOURCES AND THEIR NESTED DETAILS")
            print("-" * 80)
            if data_sources.empty:
                print("No data sources found.")
            else:
                for idx, ds_row in data_sources.iterrows():
                    ds_id = ds_row['id'] if 'id' in ds_row else ds_row.get('object_id')  # handle common id column names
                    print(f"\n🌐 Data Source: {ds_row.get('name', '')} (ID: {ds_id})")
                    # Print details of this datasource
                    detail_cols = [col for col in ['object_type','name','path','connection_string','database','schema'] if col in ds_row.index]
                    ds_details = {col: ds_row[col] for col in detail_cols}
                    for key, val in ds_details.items():
                        if pd.isna(val): continue
                        print(f"   {key.capitalize():18}: {val}")
                    
                    # Find all direct relationships where this ds is a target or source
                    # Target (eg. tables/views that belong to this DS, via 'contains' or 'connects_to')
                    nested_rel_types = ['contains', 'connects_to', 'has_table', 'has_view']
                    nested_rels_mask = relationships_df['target_object_id'] == ds_id
                    filtered_nested_rels = relationships_df[nested_rels_mask]
                    # Get direct children (eg. tables, views, schemas)
                    if not filtered_nested_rels.empty:
                        child_ids = filtered_nested_rels['source_object_id'].tolist()
                        child_objs = objects_df[objects_df['id'].isin(child_ids)] if 'id' in objects_df.columns else objects_df[objects_df['object_id'].isin(child_ids)]
                        tables = child_objs[child_objs['object_type'].str.lower().isin(['table','view','schema'])] if not child_objs.empty else pd.DataFrame()
                        if not tables.empty:
                            print(f"   - Tables/Views/Schemas contained:")
                            for _, child in tables.iterrows():
                                typ = child.get('object_type', 'Unknown')
                                nm = child.get('name', '')
                                print(f"      • {typ:10}: {nm}")
                        else:
                            print("   - No tables/views/schemas linked.")

                        # For each table, show columns if available
                        if not tables.empty:
                            for _, tbl in tables.iterrows():
                                tbl_id = tbl['id'] if 'id' in tbl and not pd.isna(tbl['id']) else tbl.get('object_id')
                                # Find columns for this table
                                col_rel_mask = (
                                    (relationships_df['source_object_id'] == tbl_id) &
                                    (relationships_df['relationship_type'].str.lower() == 'has_column')
                                )
                                if col_rel_mask.any():
                                    col_ids = relationships_df[col_rel_mask]['target_object_id'].tolist()
                                    columns = objects_df[objects_df['id'].isin(col_ids)] if 'id' in objects_df.columns else objects_df[objects_df['object_id'].isin(col_ids)]
                                    if not columns.empty:
                                        print(f"      └─ Columns for {tbl.get('name','')}:")
                                        for _, col in columns.iterrows():
                                            cname = col.get('name', '')
                                            ctype = col.get('object_type', '')
                                            dtype = col.get('data_type', col.get('properties', {}).get('data_type', ''))
                                            print(f"          - {cname:30} ({ctype}, {dtype})")
                                else:
                                    print(f"      └─ Columns for {tbl.get('name','')}: None found")
                    else:
                        print("   - No direct relationships found.")








    
        
        # Display basic info
        print(f"\n📊 DataFrames Created:")
        print(f"  Objects DataFrame: {objects_df.shape[0]} rows × {objects_df.shape[1]} columns")
        print(f"  Relationships DataFrame: {relationships_df.shape[0]} rows × {relationships_df.shape[1]} columns")
        
        # Analyze data
        analyze_data(objects_df, relationships_df)
        
        # Display sample data
        print("\n📋 SAMPLE DATA:")
        print("-" * 80)
        
        if not objects_df.empty:
            print("\nSample Objects (first 5):")
            print(objects_df[['object_type', 'name', 'path', 'complexity_score_looker']].head().to_string())
        
        if not relationships_df.empty:
            print("\nSample Relationships (first 5):")
            display_cols = ['relationship_type', 'source_name', 'target_name']
            if all(col in relationships_df.columns for col in display_cols):
                print(relationships_df[display_cols].head().to_string())
            else:
                print(relationships_df[['relationship_type', 'source_object_id', 'target_object_id']].head().to_string())
        
        # Return DataFrames for further analysis
        print("\n✅ Analysis complete! DataFrames are ready for further processing.")
        print("\n💡 You can now use objects_df and relationships_df for:")
        print("   - Generating reports")
        print("   - Data visualization")
        print("   - Exporting to CSV/Excel")
        print("   - Further analysis")
        print("\n📝 Example usage:")
        print("   objects_df.to_csv('objects.csv', index=False)")
        print("   relationships_df.to_csv('relationships.csv', index=False)")
        
        return objects_df, relationships_df
        
    except ConnectionError as e:
        print(f"\n❌ Database Connection Error:")
        print(str(e))
        return None, None
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return None, None


if __name__ == "__main__":
    objects_df, relationships_df = main()
