/*
 * Copyright 2017-2021 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micronaut.data.r2dbc.operations;

import io.micronaut.data.operations.reactive.ReactiveRepositoryOperations;

/**
 * The version of {@link ReactiveRepositoryOperations} which exposes reactor publisher types.
 *
 * @author Denis Stepanov
 * @since 2.4.0
 */
public interface ReactorReactiveRepositoryOperations extends io.micronaut.data.operations.reactive.ReactorReactiveRepositoryOperations {
}
