use std::cmp::Ordering;

use crate::storage::tuple::ScalarValue;
use crate::utils::adt::misc::compare_values_for_predicate;

pub(crate) type CompositeKey = Vec<ScalarValue>;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BTreeEntry {
    pub(crate) key: CompositeKey,
    pub(crate) offsets: Vec<usize>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum BTreeNode {
    Internal {
        entries: Vec<BTreeEntry>,
        children: Vec<Self>,
    },
    Leaf {
        entries: Vec<BTreeEntry>,
    },
}

impl BTreeNode {
    fn entry_count(&self) -> usize {
        match self {
            Self::Internal { entries, .. } | Self::Leaf { entries } => entries.len(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BTreeIndex {
    root: BTreeNode,
    max_entries: usize,
    min_entries: usize,
}

impl BTreeIndex {
    pub(crate) fn new(max_entries: usize) -> Self {
        assert!(max_entries >= 3, "B-Tree max entries must be at least 3");
        Self {
            root: BTreeNode::Leaf {
                entries: Vec::new(),
            },
            max_entries,
            min_entries: max_entries / 2,
        }
    }

    pub(crate) fn max_entries(&self) -> usize {
        self.max_entries
    }

    pub(crate) fn insert(&mut self, key: CompositeKey, offset: usize) {
        if self.root.entry_count() >= self.max_entries {
            let old_root = std::mem::replace(
                &mut self.root,
                BTreeNode::Leaf {
                    entries: Vec::new(),
                },
            );
            self.root = BTreeNode::Internal {
                entries: Vec::new(),
                children: vec![old_root],
            };
            Self::split_child(&mut self.root, 0);
        }
        Self::insert_non_full(&mut self.root, key, offset, self.max_entries);
    }

    pub(crate) fn search(&self, key: &[ScalarValue]) -> Vec<usize> {
        search_node(&self.root, key).unwrap_or_default()
    }

    #[allow(dead_code)]
    pub(crate) fn range_scan(
        &self,
        start: Option<&[ScalarValue]>,
        end: Option<&[ScalarValue]>,
    ) -> Vec<(CompositeKey, Vec<usize>)> {
        let mut out = Vec::new();
        range_scan_node(&self.root, start, end, &mut out);
        out
    }

    pub(crate) fn delete(&mut self, key: &[ScalarValue], offset: usize) -> Result<bool, String> {
        let removed = Self::delete_internal(&mut self.root, key, Some(offset), self.min_entries)?;
        self.shrink_root_if_needed();
        Ok(removed)
    }

    pub(crate) fn remap_offsets_after_deletions(&mut self, removed_offsets: &[usize]) {
        if removed_offsets.is_empty() {
            return;
        }
        remap_offsets_in_node(&mut self.root, removed_offsets);
    }

    #[allow(dead_code)]
    pub(crate) fn is_internal_root(&self) -> bool {
        matches!(self.root, BTreeNode::Internal { .. })
    }

    fn shrink_root_if_needed(&mut self) {
        if let BTreeNode::Internal { entries, children } = &mut self.root
            && entries.is_empty()
            && children.len() == 1
        {
            self.root = children.remove(0);
        }
    }

    fn insert_non_full(node: &mut BTreeNode, key: CompositeKey, offset: usize, max_entries: usize) {
        match node {
            BTreeNode::Leaf { entries } => {
                let idx = find_key_index(entries, &key);
                if idx < entries.len()
                    && composite_key_cmp(&entries[idx].key, &key) == Ordering::Equal
                {
                    if !entries[idx].offsets.contains(&offset) {
                        entries[idx].offsets.push(offset);
                    }
                    return;
                }
                entries.insert(
                    idx,
                    BTreeEntry {
                        key,
                        offsets: vec![offset],
                    },
                );
            }
            BTreeNode::Internal { entries, children } => {
                let mut idx = find_key_index(entries, &key);
                if idx < entries.len()
                    && composite_key_cmp(&entries[idx].key, &key) == Ordering::Equal
                {
                    if !entries[idx].offsets.contains(&offset) {
                        entries[idx].offsets.push(offset);
                    }
                    return;
                }

                if children[idx].entry_count() >= max_entries {
                    Self::split_child_parts(entries, children, idx);
                    match composite_key_cmp(&key, &entries[idx].key) {
                        Ordering::Greater => idx += 1,
                        Ordering::Equal => {
                            if !entries[idx].offsets.contains(&offset) {
                                entries[idx].offsets.push(offset);
                            }
                            return;
                        }
                        Ordering::Less => {}
                    }
                }
                Self::insert_non_full(&mut children[idx], key, offset, max_entries);
            }
        }
    }

    fn split_child(parent: &mut BTreeNode, index: usize) {
        if let BTreeNode::Internal { entries, children } = parent {
            Self::split_child_parts(entries, children, index);
        }
    }

    fn split_child_parts(
        entries: &mut Vec<BTreeEntry>,
        children: &mut Vec<BTreeNode>,
        index: usize,
    ) {
        let child = children.remove(index);
        let (left, median, right) = split_node(child);
        children.insert(index, left);
        children.insert(index + 1, right);
        entries.insert(index, median);
    }

    fn delete_internal(
        node: &mut BTreeNode,
        key: &[ScalarValue],
        offset: Option<usize>,
        min_entries: usize,
    ) -> Result<bool, String> {
        let (entries_len, idx, found_match, is_leaf) = match node {
            BTreeNode::Leaf { entries } => {
                let idx = find_key_index(entries, key);
                let found = idx < entries.len()
                    && composite_key_cmp(&entries[idx].key, key) == Ordering::Equal;
                (entries.len(), idx, found, true)
            }
            BTreeNode::Internal { entries, .. } => {
                let idx = find_key_index(entries, key);
                let found = idx < entries.len()
                    && composite_key_cmp(&entries[idx].key, key) == Ordering::Equal;
                (entries.len(), idx, found, false)
            }
        };

        if is_leaf {
            let BTreeNode::Leaf { entries } = node else {
                return Err("leaf branch must hold a leaf node".to_string());
            };
            if !found_match || idx >= entries_len {
                return Ok(false);
            }
            if let Some(offset_value) = offset {
                let Some(pos) = entries[idx]
                    .offsets
                    .iter()
                    .position(|existing| *existing == offset_value)
                else {
                    return Ok(false);
                };
                entries[idx].offsets.remove(pos);
                if !entries[idx].offsets.is_empty() {
                    return Ok(true);
                }
            }
            entries.remove(idx);
            return Ok(true);
        }

        let BTreeNode::Internal { entries, children } = node else {
            return Err("internal branch must hold an internal node".to_string());
        };

        if found_match {
            if let Some(offset_value) = offset {
                let Some(pos) = entries[idx]
                    .offsets
                    .iter()
                    .position(|existing| *existing == offset_value)
                else {
                    return Ok(false);
                };
                entries[idx].offsets.remove(pos);
                if !entries[idx].offsets.is_empty() {
                    return Ok(true);
                }
            }
            if children[idx].entry_count() > min_entries {
                let predecessor = max_entry(&children[idx])?;
                entries[idx] = predecessor.clone();
                return Self::delete_internal(
                    &mut children[idx],
                    &predecessor.key,
                    None,
                    min_entries,
                );
            }
            if children[idx + 1].entry_count() > min_entries {
                let successor = min_entry(&children[idx + 1])?;
                entries[idx] = successor.clone();
                return Self::delete_internal(
                    &mut children[idx + 1],
                    &successor.key,
                    None,
                    min_entries,
                );
            }
            Self::merge_children(entries, children, idx)?;
            return Self::delete_internal(&mut children[idx], key, None, min_entries);
        }

        let mut child_idx = idx;
        if children[child_idx].entry_count() == min_entries {
            child_idx = Self::fill_child(entries, children, child_idx, min_entries)?;
        }
        Self::delete_internal(&mut children[child_idx], key, offset, min_entries)
    }

    fn fill_child(
        entries: &mut Vec<BTreeEntry>,
        children: &mut Vec<BTreeNode>,
        idx: usize,
        min_entries: usize,
    ) -> Result<usize, String> {
        if idx > 0 && children[idx - 1].entry_count() > min_entries {
            Self::borrow_from_prev(entries, children, idx)?;
            return Ok(idx);
        }
        if idx + 1 < children.len() && children[idx + 1].entry_count() > min_entries {
            Self::borrow_from_next(entries, children, idx)?;
            return Ok(idx);
        }
        if idx + 1 < children.len() {
            Self::merge_children(entries, children, idx)?;
            Ok(idx)
        } else {
            Self::merge_children(entries, children, idx - 1)?;
            Ok(idx - 1)
        }
    }

    fn borrow_from_prev(
        entries: &mut [BTreeEntry],
        children: &mut [BTreeNode],
        idx: usize,
    ) -> Result<(), String> {
        let (left_slice, right_slice) = children.split_at_mut(idx);
        let left = left_slice
            .last_mut()
            .ok_or_else(|| "left sibling must exist for borrow_from_prev".to_string())?;
        let child = right_slice
            .first_mut()
            .ok_or_else(|| "child must exist for borrow_from_prev".to_string())?;

        match (left, child) {
            (
                BTreeNode::Leaf {
                    entries: left_entries,
                },
                BTreeNode::Leaf {
                    entries: child_entries,
                },
            ) => {
                let borrowed = left_entries
                    .pop()
                    .ok_or_else(|| "left sibling must have an entry to borrow".to_string())?;
                let old_parent = std::mem::replace(&mut entries[idx - 1], borrowed);
                child_entries.insert(0, old_parent);
            }
            (
                BTreeNode::Internal {
                    entries: left_entries,
                    children: left_children,
                },
                BTreeNode::Internal {
                    entries: child_entries,
                    children: child_children,
                },
            ) => {
                let borrowed = left_entries
                    .pop()
                    .ok_or_else(|| "left sibling must have an entry to borrow".to_string())?;
                let borrowed_child = left_children.pop().ok_or_else(|| {
                    "left sibling internal node must have child to borrow".to_string()
                })?;
                let old_parent = std::mem::replace(&mut entries[idx - 1], borrowed);
                child_entries.insert(0, old_parent);
                child_children.insert(0, borrowed_child);
            }
            _ => return Err("B-Tree siblings must be the same node type".to_string()),
        }
        Ok(())
    }

    fn borrow_from_next(
        entries: &mut [BTreeEntry],
        children: &mut [BTreeNode],
        idx: usize,
    ) -> Result<(), String> {
        let (left_slice, right_slice) = children.split_at_mut(idx + 1);
        let child = left_slice
            .last_mut()
            .ok_or_else(|| "child must exist for borrow_from_next".to_string())?;
        let right = right_slice
            .first_mut()
            .ok_or_else(|| "right sibling must exist for borrow_from_next".to_string())?;

        match (child, right) {
            (
                BTreeNode::Leaf {
                    entries: child_entries,
                },
                BTreeNode::Leaf {
                    entries: right_entries,
                },
            ) => {
                let borrowed = right_entries.remove(0);
                let old_parent = std::mem::replace(&mut entries[idx], borrowed);
                child_entries.push(old_parent);
            }
            (
                BTreeNode::Internal {
                    entries: child_entries,
                    children: child_children,
                },
                BTreeNode::Internal {
                    entries: right_entries,
                    children: right_children,
                },
            ) => {
                let borrowed = right_entries.remove(0);
                let borrowed_child = right_children.remove(0);
                let old_parent = std::mem::replace(&mut entries[idx], borrowed);
                child_entries.push(old_parent);
                child_children.push(borrowed_child);
            }
            _ => return Err("B-Tree siblings must be the same node type".to_string()),
        }
        Ok(())
    }

    fn merge_children(
        entries: &mut Vec<BTreeEntry>,
        children: &mut Vec<BTreeNode>,
        idx: usize,
    ) -> Result<(), String> {
        let separator = entries.remove(idx);
        let right = children.remove(idx + 1);
        let left = children
            .get_mut(idx)
            .ok_or_else(|| "left child must exist for merge".to_string())?;

        match (left, right) {
            (
                BTreeNode::Leaf {
                    entries: left_entries,
                },
                BTreeNode::Leaf {
                    entries: right_entries,
                },
            ) => {
                left_entries.push(separator);
                left_entries.extend(right_entries);
            }
            (
                BTreeNode::Internal {
                    entries: left_entries,
                    children: left_children,
                },
                BTreeNode::Internal {
                    entries: right_entries,
                    children: right_children,
                },
            ) => {
                left_entries.push(separator);
                left_entries.extend(right_entries);
                left_children.extend(right_children);
            }
            _ => return Err("B-Tree siblings must be the same node type".to_string()),
        }
        Ok(())
    }
}

fn find_key_index(entries: &[BTreeEntry], key: &[ScalarValue]) -> usize {
    entries.partition_point(|entry| composite_key_cmp(&entry.key, key) == Ordering::Less)
}

fn split_node(node: BTreeNode) -> (BTreeNode, BTreeEntry, BTreeNode) {
    match node {
        BTreeNode::Leaf { mut entries } => {
            let split_idx = entries.len() / 2;
            let right_entries = entries.split_off(split_idx + 1);
            let Some(median) = entries.pop() else {
                unreachable!("split leaf node must have a median entry");
            };
            (
                BTreeNode::Leaf { entries },
                median,
                BTreeNode::Leaf {
                    entries: right_entries,
                },
            )
        }
        BTreeNode::Internal {
            mut entries,
            mut children,
        } => {
            let split_idx = entries.len() / 2;
            let right_children = children.split_off(split_idx + 1);
            let right_entries = entries.split_off(split_idx + 1);
            let Some(median) = entries.pop() else {
                unreachable!("split internal node must have a median entry");
            };
            (
                BTreeNode::Internal { entries, children },
                median,
                BTreeNode::Internal {
                    entries: right_entries,
                    children: right_children,
                },
            )
        }
    }
}

fn search_node(node: &BTreeNode, key: &[ScalarValue]) -> Option<Vec<usize>> {
    match node {
        BTreeNode::Leaf { entries } => {
            let idx = find_key_index(entries, key);
            if idx < entries.len() && composite_key_cmp(&entries[idx].key, key) == Ordering::Equal {
                Some(entries[idx].offsets.clone())
            } else {
                None
            }
        }
        BTreeNode::Internal { entries, children } => {
            let idx = find_key_index(entries, key);
            if idx < entries.len() && composite_key_cmp(&entries[idx].key, key) == Ordering::Equal {
                Some(entries[idx].offsets.clone())
            } else {
                search_node(&children[idx], key)
            }
        }
    }
}

#[allow(dead_code)]
fn range_scan_node(
    node: &BTreeNode,
    start: Option<&[ScalarValue]>,
    end: Option<&[ScalarValue]>,
    out: &mut Vec<(CompositeKey, Vec<usize>)>,
) {
    match node {
        BTreeNode::Leaf { entries } => {
            for entry in entries {
                if within_bounds(&entry.key, start, end) {
                    out.push((entry.key.clone(), entry.offsets.clone()));
                }
            }
        }
        BTreeNode::Internal { entries, children } => {
            for (idx, entry) in entries.iter().enumerate() {
                range_scan_node(&children[idx], start, end, out);
                if within_bounds(&entry.key, start, end) {
                    out.push((entry.key.clone(), entry.offsets.clone()));
                }
            }
            if let Some(last_child) = children.last() {
                range_scan_node(last_child, start, end, out);
            } else {
                debug_assert!(false, "internal nodes must contain at least one child");
            }
        }
    }
}

#[allow(dead_code)]
fn within_bounds(
    key: &[ScalarValue],
    start: Option<&[ScalarValue]>,
    end: Option<&[ScalarValue]>,
) -> bool {
    if let Some(start_key) = start
        && composite_key_cmp(key, start_key) == Ordering::Less
    {
        return false;
    }
    if let Some(end_key) = end
        && composite_key_cmp(key, end_key) == Ordering::Greater
    {
        return false;
    }
    true
}

fn min_entry(node: &BTreeNode) -> Result<BTreeEntry, String> {
    match node {
        BTreeNode::Leaf { entries } => entries
            .first()
            .cloned()
            .ok_or_else(|| "leaf node must contain entries".to_string()),
        BTreeNode::Internal { children, .. } => min_entry(
            children
                .first()
                .ok_or_else(|| "internal node must contain children".to_string())?,
        ),
    }
}

fn max_entry(node: &BTreeNode) -> Result<BTreeEntry, String> {
    match node {
        BTreeNode::Leaf { entries } => entries
            .last()
            .cloned()
            .ok_or_else(|| "leaf node must contain entries".to_string()),
        BTreeNode::Internal { children, .. } => max_entry(
            children
                .last()
                .ok_or_else(|| "internal node must contain children".to_string())?,
        ),
    }
}

fn remap_offsets_in_node(node: &mut BTreeNode, removed_offsets: &[usize]) {
    match node {
        BTreeNode::Leaf { entries } => {
            for entry in entries {
                remap_offsets(entry, removed_offsets);
            }
        }
        BTreeNode::Internal { entries, children } => {
            for child in children {
                remap_offsets_in_node(child, removed_offsets);
            }
            for entry in entries {
                remap_offsets(entry, removed_offsets);
            }
        }
    }
}

fn remap_offsets(entry: &mut BTreeEntry, removed_offsets: &[usize]) {
    entry
        .offsets
        .retain(|offset| removed_offsets.binary_search(offset).is_err());
    for offset in &mut entry.offsets {
        let removed_before = removed_offsets.partition_point(|removed| *removed < *offset);
        *offset -= removed_before;
    }
}

pub(crate) fn composite_key_cmp(left: &[ScalarValue], right: &[ScalarValue]) -> Ordering {
    for (left_value, right_value) in left.iter().zip(right.iter()) {
        let ord = scalar_cmp(left_value, right_value);
        if ord != Ordering::Equal {
            return ord;
        }
    }
    left.len().cmp(&right.len())
}

fn scalar_cmp(left: &ScalarValue, right: &ScalarValue) -> Ordering {
    match (left, right) {
        (ScalarValue::Null, ScalarValue::Null) => Ordering::Equal,
        (ScalarValue::Null, _) => Ordering::Less,
        (_, ScalarValue::Null) => Ordering::Greater,
        _ => compare_values_for_predicate(left, right).unwrap_or(Ordering::Equal),
    }
}

#[cfg(test)]
mod tests {
    use super::{BTreeIndex, composite_key_cmp};
    use crate::storage::tuple::ScalarValue;
    use std::cmp::Ordering;

    fn int_key(value: i64) -> Vec<ScalarValue> {
        vec![ScalarValue::Int(value)]
    }

    #[test]
    fn insert_and_search_exact_keys() {
        let mut index = BTreeIndex::new(3);
        for value in 0..40 {
            index.insert(int_key(value), value as usize);
        }
        for value in 0..40 {
            assert_eq!(index.search(&int_key(value)), vec![value as usize]);
        }
        assert!(index.search(&int_key(99)).is_empty());
    }

    #[test]
    fn splits_build_internal_nodes() {
        let mut index = BTreeIndex::new(3);
        for value in 0..32 {
            index.insert(int_key(value), value as usize);
        }
        assert!(index.is_internal_root());
    }

    #[test]
    fn delete_rebalances_and_removes_keys() {
        let mut index = BTreeIndex::new(3);
        for value in 0..30 {
            index.insert(int_key(value), value as usize);
        }
        for value in (0..30).step_by(2) {
            assert!(index.delete(&int_key(value), value as usize).unwrap());
        }
        for value in (0..30).step_by(2) {
            assert!(index.search(&int_key(value)).is_empty());
        }
        for value in (1..30).step_by(2) {
            assert_eq!(index.search(&int_key(value)), vec![value as usize]);
        }
    }

    #[test]
    fn range_scan_is_sorted_and_bounded() {
        let mut index = BTreeIndex::new(3);
        for value in [7_i64, 3, 9, 4, 1, 6, 8, 2, 5] {
            index.insert(int_key(value), value as usize);
        }
        let rows = index.range_scan(Some(&int_key(3)), Some(&int_key(7)));
        let keys = rows
            .into_iter()
            .map(|(key, _)| match key.as_slice() {
                [ScalarValue::Int(value)] => *value,
                _ => panic!("unexpected key shape"),
            })
            .collect::<Vec<_>>();
        assert_eq!(keys, vec![3, 4, 5, 6, 7]);
    }

    #[test]
    fn supports_composite_keys() {
        let mut index = BTreeIndex::new(3);
        let first = vec![ScalarValue::Int(1), ScalarValue::Text("a".to_string())];
        let second = vec![ScalarValue::Int(1), ScalarValue::Text("b".to_string())];
        let third = vec![ScalarValue::Int(2), ScalarValue::Text("a".to_string())];

        index.insert(first.clone(), 10);
        index.insert(second.clone(), 20);
        index.insert(third.clone(), 30);

        assert_eq!(index.search(&first), vec![10]);
        assert_eq!(index.search(&second), vec![20]);
        assert_eq!(index.search(&third), vec![30]);
    }

    #[test]
    fn nulls_sort_first_and_allow_duplicate_keys() {
        let mut index = BTreeIndex::new(3);
        let null_key = vec![ScalarValue::Null];
        let int_key = vec![ScalarValue::Int(1)];

        index.insert(null_key.clone(), 1);
        index.insert(null_key.clone(), 2);
        index.insert(int_key.clone(), 3);

        assert_eq!(index.search(&null_key), vec![1, 2]);
        let rows = index.range_scan(None, None);
        assert_eq!(rows[0].0, null_key);
        assert_eq!(rows[1].0, int_key);
    }

    #[test]
    fn composite_comparison_puts_nulls_first() {
        let left = vec![ScalarValue::Null, ScalarValue::Int(1)];
        let right = vec![ScalarValue::Int(0), ScalarValue::Int(1)];
        assert_eq!(composite_key_cmp(&left, &right), Ordering::Less);
    }
}
