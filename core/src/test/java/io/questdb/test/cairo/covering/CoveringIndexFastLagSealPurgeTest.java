/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/


package io.questdb.test.cairo.covering;

import io.questdb.PropertyKey;
import io.questdb.cairo.PostingSealPurgeJob;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.test.AbstractCairoTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

public class CoveringIndexFastLagSealPurgeTest extends AbstractCairoTest {
    private int iteration;

    @Before
    public void setUp() {
        PostingIndexWriter.COVERING_COUNTERS_ENABLED = true;
        iteration = 1;
        setCurrentMicros(0);
        // classic partitions: merge-append disables WAL lag and with it the fast-lag publish path
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, false);
        node1.setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY, 1);
        node1.setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY_LIMIT, 10);
        node1.setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY_MULTIPLIER, 2);
    }

    @After
    public void tearDown() {
        PostingIndexWriter.COVERING_COUNTERS_ENABLED = false;
    }

    @Test
    public void testFastLagAutoSealKeepsSupersededFilesWhileReaderPinned() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (value), value DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY YEAR WAL");
            insert(0);
            drainWalQueue();
            PostingIndexWriter.COVERING_MAX_GENCOUNT_OBSERVED.set(0);
            PostingIndexWriter.COVERING_AUTOSEAL_COUNT.set(0);

            // one fast-lag commit per generation, up to one short of the auto-seal threshold
            int commits = 0;
            while (PostingIndexWriter.COVERING_MAX_GENCOUNT_OBSERVED.get() < PostingIndexUtils.MAX_GEN_COUNT - 1) {
                insert(++commits * 100);
                drainWalQueue();
            }
            Assert.assertEquals(0, PostingIndexWriter.COVERING_AUTOSEAL_COUNT.get());

            final File partitionDir = findPartitionDir();
            try (PostingSealPurgeJob job = new PostingSealPurgeJob(engine)) {
                // purge seals superseded so far, so only the head's sidecars remain
                runPurgeJob(job);
                final Set<String> headSidecars = listSidecars(partitionDir);
                Assert.assertFalse(headSidecars.isEmpty());

                try (TableReader ignored = getReader("t")) { // pinned at T
                    insert((commits + 1) * 100);
                    drainWalQueue();
                    Assert.assertEquals(1, PostingIndexWriter.COVERING_AUTOSEAL_COUNT.get());

                    runPurgeJob(job);
                    final Set<String> after = listSidecars(partitionDir);
                    for (String f : headSidecars) {
                        Assert.assertTrue("purged under a reader pinned at the txn it was published for: " + f, after.contains(f));
                    }
                }

                runPurgeJob(job);
                Assert.assertFalse("not reclaimed after the reader released", listSidecars(partitionDir).containsAll(headSidecars));
            }
        });
    }

    private File findPartitionDir() {
        final File tableDir = new File(configuration.getDbRoot().toString(), engine.verifyTableName("t").getDirName());
        for (File e : tableDir.listFiles()) {
            if (e.isDirectory() && e.getName().startsWith("2024")) {
                return e;
            }
        }
        throw new AssertionError("partition directory not found under " + tableDir);
    }

    private void insert(int firstRow) throws Exception {
        execute("INSERT INTO t SELECT dateadd('s', (" + firstRow + " + x)::INT, '2024-01-01T00:00:00Z'::TIMESTAMP),"
                + " 'S' || ((" + firstRow + " + x) % 5), (" + firstRow + " + x)::DOUBLE FROM long_sequence(10)");
    }

    private Set<String> listSidecars(File partitionDir) {
        final Set<String> out = new HashSet<>();
        for (File e : partitionDir.listFiles()) {
            if (e.getName().startsWith("sym.pv") || e.getName().startsWith("sym.pc")) {
                out.add(e.getName());
            }
        }
        return out;
    }

    private void runPurgeJob(PostingSealPurgeJob job) {
        for (int i = 0; i < 6; i++) {
            setCurrentMicros(currentMicros + 100L * iteration++);
            job.run();
        }
    }
}
