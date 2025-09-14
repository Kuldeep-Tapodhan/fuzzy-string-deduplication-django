import pandas as pd
from thefuzz import fuzz
from thefuzz import process
from django.shortcuts import render
from django.core.files.storage import FileSystemStorage
import os

def process_file_view(request):
    """
    Handles file upload, performs fuzzy matching, and renders the results.
    """
    if request.method == 'POST' and request.FILES.get('document'):
        uploaded_file = request.FILES['document']
        fs = FileSystemStorage()
        file_name = fs.save(uploaded_file.name, uploaded_file)
        file_path = fs.path(file_name)
        
        results = None
        error = None
        column_name = None
        
        try:
            # Step 1: Read the CSV file
            df = pd.read_csv(file_path)

            # Step 2: Implement flexible column detection logic
            # A. Prioritize common keywords in a case-insensitive search
            name_keywords = ['name', 'title', 'product', 'item', 'description']
            for col in df.columns:
                if any(keyword in col.lower() for keyword in name_keywords):
                    column_name = col
                    break
            
            # B. If no keyword match, use a heuristic to find the column with the most unique string values
            if not column_name:
                best_col = None
                max_unique_strings = -1
                for col in df.columns:
                    if df[col].dtype == 'object':
                        num_unique = df[col].nunique()
                        if num_unique > max_unique_strings:
                            max_unique_strings = num_unique
                            best_col = col
                column_name = best_col

            # C. As a final fallback, select the first string-based column
            if not column_name:
                for col in df.columns:
                    if df[col].dtype == 'object':
                        column_name = col
                        break

            # If no suitable column is found after all attempts, raise an error
            if not column_name:
                error = "Could not find a suitable column for deduplication. Please ensure your CSV has at least one column with text data."
                raise ValueError(error)
            
            # Step 3: Define the fuzzy matching function
            def find_fuzzy_duplicates(df, column, threshold=85):
                duplicates = []
                processed_indices = set()
                
                # Pre-process the column for matching
                df[column] = df[column].astype(str).str.lower().str.strip()
                names_to_process = list(zip(df[column], df.index))

                for i in range(len(names_to_process)):
                    name, index = names_to_process[i]
                    if index in processed_indices:
                        continue
                    
                    matches = process.extract(name, [n[0] for n in names_to_process],
                                            scorer=fuzz.token_sort_ratio)
                    
                    group_indices = []
                    for matched_name, score in matches:
                        if score >= threshold:
                            # Find the original indices for the matched name
                            original_indices = df[df[column] == matched_name].index.tolist()
                            group_indices.extend(original_indices)
                    
                    if len(set(group_indices)) > 1:
                        final_group = sorted(list(set(group_indices)))
                        if not any(idx in processed_indices for idx in final_group):
                            duplicates.append(final_group)
                            for dup_index in final_group:
                                processed_indices.add(dup_index)
                return duplicates

            # Step 4: Run the analysis and prepare the results
            duplicate_groups = find_fuzzy_duplicates(df, column_name)
            results = [df.loc[group].to_html(index=False) for group in duplicate_groups]
        
        except Exception as e:
            error = f"An unexpected error occurred: {e}"
        finally:
            if os.path.exists(file_path):
                os.remove(file_path)
        
        return render(request, 'core/home.html', {
            'results': results,
            'file_name': uploaded_file.name,
            'error': error
        })
    
    return render(request, 'core/home.html')