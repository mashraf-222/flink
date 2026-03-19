/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.operators.util;

import org.apache.flink.annotation.Internal;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;

/** Immutable unordered collection of fields IDs. */
@Internal
public class FieldSet implements Iterable<Integer> {

    public static final FieldSet EMPTY_SET = new FieldSet();

    protected final Collection<Integer> collection;
    private static final float HASHSET_LOAD_FACTOR = 0.75f;

    // --------------------------------------------------------------------------------------------

    /** Creates a new empty set of fields. */
    public FieldSet() {
        this.collection = Collections.emptySet();
    }

    /**
     * Creates a set with one field.
     *
     * @param fieldID The id of the field.
     */
    public FieldSet(Integer fieldID) {
        if (fieldID == null) {
            throw new IllegalArgumentException("Field ID must not be null.");
        }

        this.collection = Collections.singleton(fieldID);
    }

    /**
     * Creates a set with the given fields.
     *
     * @param fieldIDs The IDs of the fields.
     */
    public FieldSet(int... fieldIDs) {
        this.collection = intsToCollection(fieldIDs);
    }

    /**
     * Creates a set with the given fields.
     *
     * @param fieldIDs The IDs of the fields.
     */
    public FieldSet(int[] fieldIDs, boolean marker) {
        // marker parameter preserved for binary compatibility; behavior identical to the other ctor
        this.collection = intsToCollection(fieldIDs);
    }

    protected FieldSet(Collection<Integer> fields) {
        this.collection = fields;
    }

    /**
     * @param fieldSet The first part of the new set, NOT NULL!
     * @param fieldID The ID to be added, NOT NULL!
     */
    private FieldSet(FieldSet fieldSet, Integer fieldID) {
        if (fieldSet.size() == 0) {
            this.collection = Collections.singleton(fieldID);
        } else {
            int expectedSize = fieldSet.collection.size() + 1;
            HashSet<Integer> set = new HashSet<Integer>(capacityForSize(expectedSize));
            set.addAll(fieldSet.collection);
            set.add(fieldID);
            this.collection = Collections.unmodifiableSet(set);
        }
    }

    private FieldSet(FieldSet fieldSet, int... fieldIDs) {
        if (fieldIDs == null || fieldIDs.length == 0) {
            this.collection = fieldSet.collection;
        } else {
            int expectedSize = fieldSet.collection.size() + fieldIDs.length;
            HashSet<Integer> set =
                    new HashSet<Integer>(capacityForSize(expectedSize));
            set.addAll(fieldSet.collection);

            for (int id : fieldIDs) {
                set.add(id);
            }

            this.collection = Collections.unmodifiableSet(set);
        }
    }

    private FieldSet(FieldSet fieldSet1, FieldSet fieldSet2) {
        if (fieldSet2.size() == 0) {
            this.collection = fieldSet1.collection;
        } else if (fieldSet1.size() == 0) {
            this.collection = fieldSet2.collection;
        } else {
            int expectedSize = fieldSet1.size() + fieldSet2.size();
            HashSet<Integer> set = new HashSet<Integer>(capacityForSize(expectedSize));
            set.addAll(fieldSet1.collection);
            set.addAll(fieldSet2.collection);
            this.collection = Collections.unmodifiableSet(set);
        }
    }

    // --------------------------------------------------------------------------------------------

    public FieldSet addField(Integer fieldID) {
        if (fieldID == null) {
            throw new IllegalArgumentException("Field ID must not be null.");
        }
        return new FieldSet(this, fieldID);
    }

    public FieldSet addFields(int... fieldIDs) {
        return new FieldSet(this, fieldIDs);
    }

    public FieldSet addFields(FieldSet set) {
        if (set == null) {
            throw new IllegalArgumentException("FieldSet to add must not be null.");
        }

        if (set.size() == 0) {
            return this;
        } else if (this.size() == 0) {
            return set;
        } else {
            return new FieldSet(this, set);
        }
    }

    public boolean contains(Integer columnIndex) {
        return this.collection.contains(columnIndex);
    }

    public int size() {
        return this.collection.size();
    }

    @Override
    public Iterator<Integer> iterator() {
        return this.collection.iterator();
    }

    /**
     * Turns the FieldSet into an ordered FieldList.
     *
     * @return An ordered FieldList.
     */
    public FieldList toFieldList() {
        int[] pos = toArray();
        Arrays.sort(pos);
        return new FieldList(pos);
    }

    /**
     * Transforms the field set into an array of field IDs. Whether the IDs are ordered or unordered
     * depends on the specific subclass of the field set.
     *
     * @return An array of all contained field IDs.
     */
    public int[] toArray() {
        int[] a = new int[this.collection.size()];
        int i = 0;
        for (int col : this.collection) {
            a[i++] = col;
        }
        return a;
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Checks if the given set of fields is a valid subset of this set of fields. For unordered
     * sets, this is the case if all of the given set's fields are also part of this field.
     *
     * <p>Subclasses that describe field sets where the field order matters must override this
     * method to implement a field ordering sensitive check.
     *
     * @param set The set that is a candidate subset.
     * @return True, if the given set is a subset of this set, false otherwise.
     */
    public boolean isValidSubset(FieldSet set) {
        if (set.size() > size()) {
            return false;
        }
        for (Integer i : set) {
            if (!contains(i)) {
                return false;
            }
        }
        return true;
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public int hashCode() {
        return this.collection.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj instanceof FieldSet) {
            return this.collection.equals(((FieldSet) obj).collection);
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        StringBuilder bld = new StringBuilder();
        bld.append(getDescriptionPrefix());
        for (Integer i : this.collection) {
            bld.append(i);
            bld.append(',');
            bld.append(' ');
        }
        if (this.collection.size() > 0) {
            bld.setLength(bld.length() - 2);
        }
        bld.append(getDescriptionSuffix());
        return bld.toString();
    }

    /**
     * Since instances of FieldSet are strictly immutable, this method does not actually clone, but
     * it only returns the original instance.
     *
     * @return This objects reference, unmodified.
     */
    public FieldSet clone() {
        return this;
    }

    // --------------------------------------------------------------------------------------------

    protected String getDescriptionPrefix() {
        return "(";
    }

    protected String getDescriptionSuffix() {
        return ")";
    }

    private static int capacityForSize(int expectedSize) {
        // Ensure at least a small capacity to avoid zero or tiny values.
        // The +1 accounts for integer truncation when dividing by load factor.
        return Math.max(2, (int) (expectedSize / HASHSET_LOAD_FACTOR) + 1);
    }

    private static Collection<Integer> intsToCollection(int[] fieldIDs) {
        if (fieldIDs == null || fieldIDs.length == 0) {
            return Collections.emptySet();
        } else if (fieldIDs.length == 1) {
            return Collections.singleton(Integer.valueOf(fieldIDs[0]));
        } else {
            HashSet<Integer> set = new HashSet<Integer>(capacityForSize(fieldIDs.length));
            for (int id : fieldIDs) {
                set.add(id);
            }
            return Collections.unmodifiableSet(set);
        }
    }

}
