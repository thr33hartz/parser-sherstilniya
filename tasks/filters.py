def apply_pnl_filters(df: pd.DataFrame, filters: dict) -> pd.DataFrame:
    """Применяет сохраненные PNL-фильтры к DataFrame."""
    if not filters:
        return df

    filtered_df = df.copy()
    for column, rules in filters.items():
        if column not in filtered_df.columns:
            continue
        
        # Убедимся, что колонка числовая, игнорируя ошибки
        filtered_df[column] = pd.to_numeric(filtered_df[column], errors='coerce')
        
        min_val = rules.get('min')
        max_val = rules.get('max')

        if min_val is not None:
            filtered_df = filtered_df[filtered_df[column] >= min_val]
        if max_val is not None:
            filtered_df = filtered_df[filtered_df[column] <= max_val]
            
    return filtered_df