/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.uid.Versions;

import java.io.IOException;

/**
 * <h2>核心职责</h2>
 * <p>定义和管理 Elasticsearch 的文档版本控制策略，提供三种不同的版本类型来控制文档更新的并发行为和版本冲突检测机制。</p>
 *
 * <h2>工作机制</h2>
 * <p>作为版本策略的枚举抽象类，每个版本类型（INTERNAL、EXTERNAL、EXTERNAL_GTE）实现了统一的版本冲突检测、版本更新和版本验证接口。系统根据用户指定的版本类型，采用不同的算法来判断是否允许文档更新，确保分布式环境下的数据一致性。本质上是一种策略模式（Strategy Pattern）的实现。</p>
 *
 * <h2>架构位置</h2>
 * <p>属于 org.elasticsearch.index 模块，位于索引层（indices）。在文档索引、更新、删除操作中被使用，特别是在 IndexShard 和 InternalEngine 中进行版本控制。通常由请求参数（如 IndexRequest）指定版本类型，并在写入路径中被引擎调用。</p>
 *
 * <h2>关键设计点</h2>
 * <ul>
 * <li><b>策略模式</b>：通过枚举实现不同版本类型的具体算法，运行时根据类型选择策略</li>
 * <li><b>双向控制</b>：区分写入（Writes）和读取（Reads）两种场景的版本控制逻辑</li>
 * <li><b>特殊版本值</b>：支持 MATCH_ANY、MATCH_DELETED、NOT_FOUND 等特殊语义版本</li>
 * <li><b>序列化支持</b>：实现 Writeable 接口，支持网络传输和持久化</li>
 * <li><b>严格的输入验证</b>：每种版本类型都有独立的 validate 方法确保版本值合法</li>
 * </ul>
 *
 * <h2>三种版本类型概览</h2>
 * <ul>
 * <li><b>INTERNAL</b>：默认版本类型，使用 Elasticsearch 内部版本号递增机制，适用于常规的乐观锁控制</li>
 * <li><b>EXTERNAL</b>：使用外部版本号（如数据库时间戳），要求提供的版本号严格大于当前版本</li>
 * <li><b>EXTERNAL_GTE</b>：允许外部版本号大于或等于当前版本，适用于某些特定场景的数据同步</li>
 * </ul>
 */
public enum VersionType implements Writeable {
    INTERNAL((byte) 0) {
        @Override
        public boolean isVersionConflictForWrites(long currentVersion, long expectedVersion, boolean deleted) {
            return isVersionConflict(currentVersion, expectedVersion, deleted);
        }

        @Override
        public String explainConflictForWrites(long currentVersion, long expectedVersion, boolean deleted) {
            if (expectedVersion == Versions.MATCH_DELETED) {
                return "document already exists (current version [" + currentVersion + "])";
            }
            if (currentVersion == Versions.NOT_FOUND) {
                return "document does not exist (expected version [" + expectedVersion + "])";
            }
            return "current version [" + currentVersion + "] is different than the one provided [" + expectedVersion + "]";
        }

        @Override
        public boolean isVersionConflictForReads(long currentVersion, long expectedVersion) {
            return isVersionConflict(currentVersion, expectedVersion, false);
        }

        @Override
        public String explainConflictForReads(long currentVersion, long expectedVersion) {
            if (currentVersion == Versions.NOT_FOUND) {
                return "document does not exist (expected version [" + expectedVersion + "])";
            }
            return "current version [" + currentVersion + "] is different than the one provided [" + expectedVersion + "]";
        }

        /**
         * <h2>核心作用</h2>
         * <p>检查当前版本与期望版本是否冲突，是 INTERNAL 版本类型版本控制的核心判断逻辑。</p>
         *
         * <h2>输入与输出</h2>
         * <p><b>输入</b>：currentVersion（文档当前版本）、expectedVersion（操作指定的期望版本）、deleted（文档是否被删除）<br>
         * <b>输出</b>：返回 true 表示存在冲突，更新操作将被拒绝；false 表示无冲突，允许更新</p>
         *
         * <h2>使用场景</h2>
         * <p>在索引、更新或删除文档时被调用，用于乐观锁控制。确保只有持有正确版本号的客户端才能成功修改文档。</p>
         *
         * <h2>异常与边界</h2>
         * <p>处理三种特殊情况：MATCH_ANY（忽略版本）、MATCH_DELETED（期望文档已删除）、NOT_FOUND（文档不存在）。</p>
         *
         * <h2>关键逻辑</h2>
         * <p>采用精确匹配策略：只有当期望版本与当前版本完全一致时才允许操作，否则视为冲突。这是经典的乐观锁实现。</p>
         */
        private static boolean isVersionConflict(long currentVersion, long expectedVersion, boolean deleted) {
            if (expectedVersion == Versions.MATCH_ANY) {
                return false;
            }
            if (expectedVersion == Versions.MATCH_DELETED) {
                return deleted == false;
            }
            if (currentVersion != expectedVersion) {
                return true;
            }
            return false;
        }

        /**
         * <h2>核心作用</h2>
         * <p>计算文档更新后的新版本号，实现 Elasticsearch 内部版本的自动递增机制。</p>
         *
         * <h2>输入与输出</h2>
         * <p><b>输入</b>：currentVersion（文档当前版本）、expectedVersion（操作指定的期望版本）<br>
         * <b>输出</b>：返回递增后的新版本号</p>
         *
         * <h2>使用场景</h2>
         * <p>每次成功的索引、更新或删除操作后调用，用于生成新文档的版本号。</p>
         *
         * <h2>异常与边界</h2>
         * <p>如果文档不存在（currentVersion == NOT_FOUND），新版本号从 1 开始。</p>
         *
         * <h2>关键逻辑</h2>
         * <p>采用简单的加 1 递增策略，确保版本号的严格单调递增性，便于追踪文档的变更历史。</p>
         */
        @Override
        public long updateVersion(long currentVersion, long expectedVersion) {
            return currentVersion == Versions.NOT_FOUND ? 1 : currentVersion + 1;
        }

        /**
         * <h2>核心作用</h2>
         * <p>验证用户提供的版本号是否符合 INTERNAL 版本类型的约束要求。</p>
         *
         * <h2>输入与输出</h2>
         * <p><b>输入</b>：version（待验证的版本号）<br>
         * <b>输出</b>：返回 true 表示版本号合法，false 表示不合法</p>
         *
         * <h2>使用场景</h2>
         * <p>在索引或更新请求中，当指定 version 参数时进行校验，拒绝非法的版本号。</p>
         *
         * <h2>异常与边界</h2>
         * <p>不允许负数，不允许 NOT_FOUND（这是内部值，不是用户可指定的）。允许特殊值 MATCH_ANY 和 MATCH_DELETED。</p>
         *
         * <h2>关键逻辑</h2>
         * <p>采用严格验证策略，确保用户只能指定正整数版本号或特殊的匹配标志。</p>
         */
        @Override
        public boolean validateVersionForWrites(long version) {
            return version > 0L || version == Versions.MATCH_ANY || version == Versions.MATCH_DELETED;
        }

        /**
         * <h2>核心作用</h2>
         * <p>验证读取操作时的版本号参数是否合法。</p>
         *
         * <h2>输入与输出</h2>
         * <p><b>输入</b>：version（待验证的版本号）<br>
         * <b>输出</b>：返回 true 表示版本号合法，false 表示不合法</p>
         *
         * <h2>使用场景</h2>
         * <p>在按版本号查询文档或进行版本检查时调用。</p>
         *
         * <h2>异常与边界</h2>
         * <p>不允许 NOT_FOUND（文档不存在是运行时状态，不是查询参数）。不允许负数。允许 MATCH_ANY。</p>
         *
         * <h2>关键逻辑</h2>
         * <p>比写入验证更严格，不允许 MATCH_DELETED（删除检查只在写入时有效）。</p>
         */
        @Override
        public boolean validateVersionForReads(long version) {
            // not allowing Versions.NOT_FOUND as it is not a valid input value.
            return version > 0L || version == Versions.MATCH_ANY;
        }
    },
    EXTERNAL((byte) 1) {
        /**
         * <h2>核心作用</h2>
         * <p>检查外部版本冲突，确保提供的版本号严格大于当前版本。</p>
         *
         * <h2>输入与输出</h2>
         * <p><b>输入</b>：currentVersion（文档当前版本）、expectedVersion（外部版本号）、deleted（文档是否被删除）<br>
         * <b>输出</b>：返回 true 表示存在冲突（版本号不够大），false 表示允许更新</p>
         *
         * <h2>使用场景</h2>
         * <p>用于从外部系统（如数据库）同步数据到 Elasticsearch，使用外部系统的版本号或时间戳。</p>
         *
         * <h2>异常与边界</h2>
         * <p>文档不存在时（NOT_FOUND）直接允许更新。不允许 MATCH_ANY（外部版本必须明确指定）。</p>
         *
         * <h2>关键逻辑</h2>
         * <p>采用"严格递增"策略：期望版本必须严格大于当前版本（currentVersion >= expectedVersion 时冲突）。这确保了只有更新的数据才能覆盖旧数据。</p>
         */
        @Override
        public boolean isVersionConflictForWrites(long currentVersion, long expectedVersion, boolean deleted) {
            if (currentVersion == Versions.NOT_FOUND) {
                return false;
            }
            if (expectedVersion == Versions.MATCH_ANY) {
                return true;
            }
            if (currentVersion >= expectedVersion) {
                return true;
            }
            return false;
        }

        /**
         * <h2>核心作用</h2>
         * <p>生成外部版本冲突的人类可读错误信息。</p>
         *
         * <h2>输入与输出</h2>
         * <p><b>输入</b>：currentVersion、expectedVersion、deleted<br>
         * <b>输出</b>：描述冲突原因的字符串</p>
         *
         * <h2>使用场景</h2>
         * <p>当版本冲突检测返回 true 时，向用户返回明确的错误提示。</p>
         *
         * <h2>异常与边界</h2>
         * <p>无需区分文档不存在的情况（已在上游处理）。</p>
         *
         * <h2>关键逻辑</h2>
         * <p>强调"当前版本更高或相等"这一冲突原因，帮助用户理解外部版本号必须递增。</p>
         */
        @Override
        public String explainConflictForWrites(long currentVersion, long expectedVersion, boolean deleted) {
            return "current version [" + currentVersion + "] is higher or equal to the one provided [" + expectedVersion + "]";
        }

        /**
         * <h2>核心作用</h2>
         * <p>检查外部版本的读取冲突，要求版本号精确匹配。</p>
         *
         * <h2>输入与输出</h2>
         * <p><b>输入</b>：currentVersion、expectedVersion<br>
         * <b>输出</b>：返回 true 表示冲突，false 表示匹配成功</p>
         *
         * <h2>使用场景</h2>
         * <p>按指定版本号读取文档时使用。</p>
         *
         * <h2>异常与边界</h2>
         * <p>文档不存在视为冲突。MATCH_ANY 表示忽略版本检查。</p>
         *
         * <h2>关键逻辑</h2>
         * <p>采用精确匹配策略，与写入时的递增策略不同，读取时需要版本号完全一致。</p>
         */
        @Override
        public boolean isVersionConflictForReads(long currentVersion, long expectedVersion) {
            if (expectedVersion == Versions.MATCH_ANY) {
                return false;
            }
            if (currentVersion == Versions.NOT_FOUND) {
                return true;
            }
            if (currentVersion != expectedVersion) {
                return true;
            }
            return false;
        }

        /**
         * <h2>核心作用</h2>
         * <p>生成外部版本读取冲突的错误信息。</p>
         *
         * <h2>输入与输出</h2>
         * <p><b>输入</b>：currentVersion、expectedVersion<br>
         * <b>输出</b>：冲突描述字符串</p>
         *
         * <h2>使用场景</h2>
         * <p>读取操作版本冲突时返回给用户。</p>
         *
         * <h2>异常与边界</h2>
         * <p>区分文档不存在和版本不匹配两种情况。</p>
         *
         * <h2>关键逻辑</h2>
         * <p>提供清晰的错误信息，帮助用户定位问题。</p>
         */
        @Override
        public String explainConflictForReads(long currentVersion, long expectedVersion) {
            if (currentVersion == Versions.NOT_FOUND) {
                return "document does not exist (expected version [" + expectedVersion + "])";
            }
            return "current version [" + currentVersion + "] is different than the one provided [" + expectedVersion + "]";
        }

        /**
         * <h2>核心作用</h2>
         * <p>使用外部版本号更新文档版本，保留用户提供的外部版本号。</p>
         *
         * <h2>输入与输出</h2>
         * <p><b>输入</b>：currentVersion、expectedVersion<br>
         * <b>输出</b>：返回 expectedVersion（外部版本号）</p>
         *
         * <h2>使用场景</h2>
         * <p>每次成功更新后，将文档的版本号设置为用户指定的外部版本号。</p>
         *
         * <h2>异常与边界</h2>
         * <p>忽略当前版本号，直接使用期望版本号。</p>
         *
         * <h2>关键逻辑</h2>
         * <p>直接赋值策略，允许外部系统控制版本号，便于数据溯源和同步。</p>
         */
        @Override
        public long updateVersion(long currentVersion, long expectedVersion) {
            return expectedVersion;
        }

        /**
         * <h2>核心作用</h2>
         * <p>验证外部版本号的合法性，允许非负数。</p>
         *
         * <h2>输入与输出</h2>
         * <p><b>输入</b>：version<br>
         * <b>输出</b>：返回 true 表示合法</p>
         *
         * <h2>使用场景</h2>
         * <p>在外部版本写入操作前验证版本号。</p>
         *
         * <h2>异常与边界</h2>
         * <p>不允许负数，但允许 0。不允许 MATCH_ANY 和 MATCH_DELETED。</p>
         *
         * <h2>关键逻辑</h2>
         * <p>宽松验证策略，只要是非负数即可接受，因为外部版本可能来自不同系统。</p>
         */
        @Override
        public boolean validateVersionForWrites(long version) {
            return version >= 0L;
        }

        /**
         * <h2>核心作用</h2>
         * <p>验证外部版本的读取版本号合法性。</p>
         *
         * <h2>输入与输出</h2>
         * <p><b>输入</b>：version<br>
         * <b>输出</b>：返回 true 表示合法</p>
         *
         * <h2>使用场景</h2>
         * <p>外部版本读取操作前验证。</p>
         *
         * <h2>异常与边界</h2>
         * <p>允许非负数或 MATCH_ANY。</p>
         *
         * <h2>关键逻辑</h2>
         * <p>与写入验证一致，保证一致性。</p>
         */
        @Override
        public boolean validateVersionForReads(long version) {
            return version >= 0L || version == Versions.MATCH_ANY;
        }

    },
    EXTERNAL_GTE((byte) 2) {
        /**
         * <h2>核心作用</h2>
         * <p>检查外部版本冲突，允许提供的版本号大于或等于当前版本。</p>
         *
         * <h2>输入与输出</h2>
         * <p><b>输入</b>：currentVersion、expectedVersion、deleted<br>
         * <b>输出</b>：返回 true 表示冲突（期望版本小于当前版本）</p>
         *
         * <h2>使用场景</h2>
         * <p>适用于需要覆盖相同版本数据的场景，如数据重放或幂等操作。</p>
         *
         * <h2>异常与边界</h2>
         * <p>文档不存在时直接允许更新。不允许 MATCH_ANY。</p>
         *
         * <h2>关键逻辑</h2>
         * <p>采用"非递减"策略：只有在期望版本严格小于当前版本时才冲突（currentVersion > expectedVersion）。这与 EXTERNAL 的"严格递增"策略不同，允许版本号相同，支持幂等更新。</p>
         */
        @Override
        public boolean isVersionConflictForWrites(long currentVersion, long expectedVersion, boolean deleted) {
            if (currentVersion == Versions.NOT_FOUND) {
                return false;
            }
            if (expectedVersion == Versions.MATCH_ANY) {
                return true;
            }
            if (currentVersion > expectedVersion) {
                return true;
            }
            return false;
        }

        /**
         * <h2>核心作用</h2>
         * <p>生成 EXTERNAL_GTE 版本冲突的错误信息。</p>
         *
         * <h2>输入与输出</h2>
         * <p><b>输入</b>：currentVersion、expectedVersion、deleted<br>
         * <b>输出</b>：冲突描述字符串</p>
         *
         * <h2>使用场景</h2>
         * <p>版本冲突时向用户报告错误原因。</p>
         *
         * <h2>异常与边界</h2>
         * <p>强调"当前版本更高"，而非"更高或相等"（因为相等是允许的）。</p>
         *
         * <h2>关键逻辑</h2>
         * <p>清晰的错误信息帮助用户理解 EXTERNAL_GTE 与 EXTERNAL 的区别。</p>
         */
        @Override
        public String explainConflictForWrites(long currentVersion, long expectedVersion, boolean deleted) {
            return "current version [" + currentVersion + "] is higher than the one provided [" + expectedVersion + "]";
        }

        /**
         * <h2>核心作用</h2>
         * <p>检查 EXTERNAL_GTE 的读取版本冲突，要求精确匹配。</p>
         *
         * <h2>输入与输出</h2>
         * <p><b>输入</b>：currentVersion、expectedVersion<br>
         * <b>输出</b>：返回 true 表示冲突</p>
         *
         * <h2>使用场景</h2>
         * <p>按版本号读取文档时使用。</p>
         *
         * <h2>异常与边界</h2>
         * <p>与 EXTERNAL 逻辑完全一致，读取时仍要求精确匹配。</p>
         *
         * <h2>关键逻辑</h2>
         * <p>精确匹配策略，确保读取指定版本的数据。</p>
         */
        @Override
        public boolean isVersionConflictForReads(long currentVersion, long expectedVersion) {
            if (expectedVersion == Versions.MATCH_ANY) {
                return false;
            }
            if (currentVersion == Versions.NOT_FOUND) {
                return true;
            }
            if (currentVersion != expectedVersion) {
                return true;
            }
            return false;
        }

        /**
         * <h2>核心作用</h2>
         * <p>生成 EXTERNAL_GTE 读取冲突的错误信息。</p>
         *
         * <h2>输入与输出</h2>
         * <p><b>输入</b>：currentVersion、expectedVersion<br>
         * <b>输出</b>：冲突描述字符串</p>
         *
         * <h2>使用场景</h2>
         * <p>读取冲突时返回给用户。</p>
         *
         * <h2>异常与边界</h2>
         * <p>与 EXTERNAL 完全相同。</p>
         *
         * <h2>关键逻辑</h2>
         * <p>提供一致的用户体验。</p>
         */
        @Override
        public String explainConflictForReads(long currentVersion, long expectedVersion) {
            if (currentVersion == Versions.NOT_FOUND) {
                return "document does not exist (expected version [" + expectedVersion + "])";
            }
            return "current version [" + currentVersion + "] is different than the one provided [" + expectedVersion + "]";
        }

        /**
         * <h2>核心作用</h2>
         * <p>使用外部版本号更新文档版本，与 EXTERNAL 行为一致。</p>
         *
         * <h2>输入与输出</h2>
         * <p><b>输入</b>：currentVersion、expectedVersion<br>
         * <b>输出</b>：返回 expectedVersion</p>
         *
         * <h2>使用场景</h2>
         * <p>更新文档版本号。</p>
         *
         * <h2>异常与边界</h2>
         * <p>与 EXTERNAL 逻辑完全一致。</p>
         *
         * <h2>关键逻辑</h2>
         * <p>直接使用外部版本号，保留原始版本信息。</p>
         */
        @Override
        public long updateVersion(long currentVersion, long expectedVersion) {
            return expectedVersion;
        }

        /**
         * <h2>核心作用</h2>
         * <p>验证 EXTERNAL_GTE 版本号的合法性。</p>
         *
         * <h2>输入与输出</h2>
         * <p><b>输入</b>：version<br>
         * <b>输出</b>：返回 true 表示合法</p>
         *
         * <h2>使用场景</h2>
         * <p>写入操作前验证版本号。</p>
         *
         * <h2>异常与边界</h2>
         * <p>与 EXTERNAL 完全相同。</p>
         *
         * <h2>关键逻辑</h2>
         * <p>允许非负数，保持与 EXTERNAL 一致性。</p>
         */
        @Override
        public boolean validateVersionForWrites(long version) {
            return version >= 0L;
        }

        /**
         * <h2>核心作用</h2>
         * <p>验证 EXTERNAL_GTE 读取版本号的合法性。</p>
         *
         * <h2>输入与输出</h2>
         * <p><b>输入</b>：version<br>
         * <b>输出</b>：返回 true 表示合法</p>
         *
         * <h2>使用场景</h2>
         * <p>读取操作前验证版本号。</p>
         *
         * <h2>异常与边界</h2>
         * <p>与 EXTERNAL 完全相同。</p>
         *
         * <h2>关键逻辑</h2>
         * <p>保持验证逻辑的一致性。</p>
         */
        @Override
        public boolean validateVersionForReads(long version) {
            return version >= 0L || version == Versions.MATCH_ANY;
        }

    };

    private final byte value;

    VersionType(byte value) {
        this.value = value;
    }

    public byte getValue() {
        return value;
    }

    /**
     * <h2>核心作用</h2>
     * <p>检查在写入操作时当前文档版本与期望版本是否冲突。</p>
     *
     * <h2>输入与输出</h2>
     * <p><b>输入</b>：currentVersion（文档当前版本）、expectedVersion（期望版本）、deleted（文档是否被删除）<br>
     * <b>输出</b>：返回 true 表示存在冲突，更新操作将被拒绝</p>
     *
     * <h2>使用场景</h2>
     * <p>在索引、更新、删除文档时被调用，用于乐观锁控制，防止并发冲突。</p>
     *
     * <h2>异常与边界</h2>
     * <p>deleted 参数用于区分文档刚被删除但版本号可能还未清除的情况。</p>
     *
     * <h2>关键逻辑</h2>
     * <p>具体行为由各版本类型子类实现，策略包括精确匹配（INTERNAL）、严格递增（EXTERNAL）、非递减（EXTERNAL_GTE）。</p>
     */
    public abstract boolean isVersionConflictForWrites(long currentVersion, long expectedVersion, boolean deleted);

    /**
     * <h2>核心作用</h2>
     * <p>返回写入操作版本冲突的人类可读错误说明。</p>
     *
     * <h2>输入与输出</h2>
     * <p><b>输入</b>：currentVersion、expectedVersion、deleted<br>
     * <b>输出</b>：冲突原因描述字符串</p>
     *
     * <h2>使用场景</h2>
     * <p>仅在 isVersionConflictForWrites 返回 true 时调用，向用户返回明确的错误信息。</p>
     *
     * <h2>异常与边界</h2>
     * <p>无需重复检查冲突状态，直接生成说明文本。</p>
     *
     * <h2>关键逻辑</h2>
     * <p>根据不同的版本类型和冲突原因生成相应的错误提示，帮助用户理解问题。</p>
     *
     * Note that this method is only called if {@link #isVersionConflictForWrites(long, long, boolean)} returns true;
     *
     * @param currentVersion  the current version for the document
     * @param expectedVersion the version specified for the write operation
     * @param deleted         true if the document is currently deleted (note that #currentVersion will typically be
     *                        {@link Versions#NOT_FOUND}, but may be something else if the document was recently deleted
     */
    public abstract String explainConflictForWrites(long currentVersion, long expectedVersion, boolean deleted);

    /**
     * <h2>核心作用</h2>
     * <p>检查在读取操作时当前文档版本与期望版本是否冲突。</p>
     *
     * <h2>输入与输出</h2>
     * <p><b>输入</b>：currentVersion、expectedVersion<br>
     * <b>输出</b>：返回 true 表示存在冲突</p>
     *
     * <h2>使用场景</h2>
     * <p>在按版本号查询文档或进行版本检查时调用。</p>
     *
     * <h2>异常与边界</h2>
     * <p>不包含 deleted 参数，因为读取操作通常不关心文档是否被删除。</p>
     *
     * <h2>关键逻辑</h2>
     * <p>对于内部版本和外部版本，读取时都要求版本号精确匹配（除非是 MATCH_ANY）。</p>
     */
    public abstract boolean isVersionConflictForReads(long currentVersion, long expectedVersion);

    /**
     * <h2>核心作用</h2>
     * <p>返回读取操作版本冲突的人类可读错误说明。</p>
     *
     * <h2>输入与输出</h2>
     * <p><b>输入</b>：currentVersion、expectedVersion<br>
     * <b>输出</b>：冲突原因描述字符串</p>
     *
     * <h2>使用场景</h2>
     * <p>仅在 isVersionConflictForReads 返回 true 时调用。</p>
     *
     * <h2>异常与边界</h2>
     * <p>区分文档不存在和版本不匹配两种情况。</p>
     *
     * <h2>关键逻辑</h2>
     * <p>提供清晰的错误信息，帮助用户调试版本相关问题。</p>
     *
     * Note that this method is only called if {@link #isVersionConflictForReads(long, long)} returns true;
     *
     * @param currentVersion  the current version for the document
     * @param expectedVersion the version specified for the read operation
     */
    public abstract String explainConflictForReads(long currentVersion, long expectedVersion);

    /**
     * <h2>核心作用</h2>
     * <p>计算文档更新后的新版本号。</p>
     *
     * <h2>输入与输出</h2>
     * <p><b>输入</b>：currentVersion（文档当前版本）、expectedVersion（期望版本）<br>
     * <b>输出</b>：新版本号</p>
     *
     * <h2>使用场景</h2>
     * <p>每次成功写入操作后调用，更新文档的版本号。</p>
     *
     * <h2>异常与边界</h2>
     * <p>不同版本类型的更新策略不同：INTERNAL 递增，EXTERNAL/EXTERNAL_GTE 使用期望版本。</p>
     *
     * <h2>关键逻辑</h2>
     * <p>INTERNAL 采用当前版本+1；外部版本直接使用用户提供的版本号。</p>
     *
     * Returns the new version for a document, based on its current one and the specified in the request
     *
     * @return new version
     */
    public abstract long updateVersion(long currentVersion, long expectedVersion);

    /**
     * <h2>核心作用</h2>
     * <p>验证写入操作时的版本号是否合法。</p>
     *
     * <h2>输入与输出</h2>
     * <p><b>输入</b>：version（待验证的版本号）<br>
     * <b>输出</b>：返回 true 表示合法</p>
     *
     * <h2>使用场景</h2>
     * <p>在索引或更新请求中，当指定 version 参数时进行校验。</p>
     *
     * <h2>异常与边界</h2>
     * <p>不同版本类型有不同的合法范围：INTERNAL 要求正整数，外部版本允许非负数。</p>
     *
     * <h2>关键逻辑</h2>
     * <p>确保用户提供的版本号符合版本类型的要求，防止非法操作。</p>
     *
     * validate the version is a valid value for this type when writing.
     *
     * @return true if valid, false o.w
     */
    public abstract boolean validateVersionForWrites(long version);

    /**
     * <h2>核心作用</h2>
     * <p>验证读取操作时的版本号是否合法。</p>
     *
     * <h2>输入与输出</h2>
     * <p><b>输入</b>：version（待验证的版本号）<br>
     * <b>输出</b>：返回 true 表示合法</p>
     *
     * <h2>使用场景</h2>
     * <p>在按版本号查询文档前验证版本号参数。</p>
     *
     * <h2>异常与边界</h2>
     * <p>通常允许额外的 MATCH_ANY 标志。</p>
     *
     * <h2>关键逻辑</h2>
     * <p>与写入验证类似，但可能更宽松（允许 MATCH_ANY）。</p>
     *
     * validate the version is a valid value for this type when reading.
     *
     * @return true if valid, false o.w
     */
    public abstract boolean validateVersionForReads(long version);

    /**
     * <h2>核心作用</h2>
     * <p>将字符串类型的版本名称转换为 VersionType 枚举。</p>
     *
     * <h2>输入与输出</h2>
     * <p><b>输入</b>：versionType（字符串，如 "internal", "external"）<br>
     * <b>输出</b>：对应的 VersionType 枚举值</p>
     *
     * <h2>使用场景</h2>
     * <p>从 HTTP 请求参数或配置文件中解析版本类型。</p>
     *
     * <h2>异常与边界</h2>
     * <p>支持 "external_gt" 作为 "external" 的别名。不认识的字符串会抛出 IllegalArgumentException。</p>
     *
     * <h2>关键逻辑</h2>
     * <p>简单的字符串匹配转换，提供灵活的名称映射。</p>
     */
    public static VersionType fromString(String versionType) {
        if ("internal".equals(versionType)) {
            return INTERNAL;
        } else if ("external".equals(versionType)) {
            return EXTERNAL;
        } else if ("external_gt".equals(versionType)) {
            return EXTERNAL;
        } else if ("external_gte".equals(versionType)) {
            return EXTERNAL_GTE;
        }
        throw new IllegalArgumentException("No version type match [" + versionType + "]");
    }

    /**
     * <h2>核心作用</h2>
     * <p>将字符串类型转换为 VersionType，支持默认值。</p>
     *
     * <h2>输入与输出</h2>
     * <p><b>输入</b>：versionType（字符串）、defaultVersionType（默认版本类型）<br>
     * <b>输出</b>：转换后的 VersionType 或默认值</p>
     *
     * <h2>使用场景</h2>
     * <p>当版本类型参数可选时使用。</p>
     *
     * <h2>异常与边界</h2>
     * <p>如果 versionType 为 null，返回默认值；否则调用 fromString 转换。</p>
     *
     * <h2>关键逻辑</h2>
     * <p>提供便捷的空值处理，避免调用方手动判断。</p>
     */
    public static VersionType fromString(String versionType, VersionType defaultVersionType) {
        if (versionType == null) {
            return defaultVersionType;
        }
        return fromString(versionType);
    }

    /**
     * <h2>核心作用</h2>
     * <p>将 VersionType 枚举转换为字符串表示。</p>
     *
     * <h2>输入与输出</h2>
     * <p><b>输入</b>：versionType<br>
     * <b>输出</b>：小写的字符串表示</p>
     *
     * <h2>使用场景</h2>
     * <p>序列化、日志输出、响应返回等场景。</p>
     *
     * <h2>异常与边界</h2>
     * <p>使用 switch 表达式，确保覆盖所有枚举值。</p>
     *
     * <h2>关键逻辑</h2>
     * <p>反向转换，与 fromString 对应。</p>
     */
    public static String toString(VersionType versionType) {
        return switch (versionType) {
            case INTERNAL -> "internal";
            case EXTERNAL -> "external";
            case EXTERNAL_GTE -> "external_gte";
        };
    }

    /**
     * <h2>核心作用</h2>
     * <p>将字节值转换为 VersionType 枚举。</p>
     *
     * <h2>输入与输出</h2>
     * <p><b>输入</b>：value（字节值，0=INTERNAL, 1=EXTERNAL, 2=EXTERNAL_GTE）<br>
     * <b>输出</b>：对应的 VersionType 枚举</p>
     *
     * <h2>使用场景</h2>
     * <p>从网络协议或存储格式中反序列化版本类型。</p>
     *
     * <h2>异常与边界</h2>
     * <p>无法识别的值会抛出 IllegalArgumentException。</p>
     *
     * <h2>关键逻辑</h2>
     * <p>基于枚举序数的字节值进行映射，对应 getValue() 的反向操作。</p>
     */
    public static VersionType fromValue(byte value) {
        if (value == 0) {
            return INTERNAL;
        } else if (value == 1) {
            return EXTERNAL;
        } else if (value == 2) {
            return EXTERNAL_GTE;
        }
        throw new IllegalArgumentException("No version type match [" + value + "]");
    }

    /**
     * <h2>核心作用</h2>
     * <p>从输入流中读取 VersionType 枚举。</p>
     *
     * <h2>输入与输出</h2>
     * <p><b>输入</b>：in（StreamInput）<br>
     * <b>输出</b>：从流中读取的 VersionType</p>
     *
     * <h2>使用场景</h2>
     * <p>网络协议反序列化，如节点间通信。</p>
     *
     * <h2>异常与边界</h2>
     * <p>可能抛出 IOException。</p>
     *
     * <h2>关键逻辑</h2>
     * <p>委托给 StreamInput 的 readEnum 方法处理。</p>
     */
    public static VersionType readFromStream(StreamInput in) throws IOException {
        return in.readEnum(VersionType.class);
    }

    /**
     * <h2>核心作用</h2>
     * <p>将 VersionType 枚举写入输出流。</p>
     *
     * <h2>输入与输出</h2>
     * <p><b>输入</b>：out（StreamOutput）<br>
     * <b>输出</b>：无</p>
     *
     * <h2>使用场景</h2>
     * <p>网络协议序列化，如节点间通信。</p>
     *
     * <h2>异常与边界</h2>
     * <p>可能抛出 IOException。</p>
     *
     * <h2>关键逻辑</h2>
     * <p>委托给 StreamOutput 的 writeEnum 方法处理。</p>
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(this);
    }
}
