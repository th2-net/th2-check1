/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package com.exactpro.th2.verifier;

import java.util.concurrent.TimeUnit;

import org.apache.poi.ss.formula.functions.T;

import io.reactivex.Observable;
import io.reactivex.annotations.NonNull;
import io.reactivex.disposables.Disposable;
import io.reactivex.observables.ConnectableObservable;
import io.reactivex.observers.DisposableObserver;
import io.reactivex.subjects.SingleSubject;
import io.reactivex.subjects.Subject;
import kotlin.Pair;
import kotlin.Triple;

public class Test {

    public static void main(String[] args) throws InterruptedException {

        Observable<Long> b = Subject.intervalRange(0, 10, 0, 500, TimeUnit.MILLISECONDS)
                .publish()
                .autoConnect();

        var c = new DisposableObserver<Long>() {

            @Override
            public void onError(@NonNull Throwable e) {
                System.out.println("error");
            }

            @Override
            public void onComplete() {
                System.out.println("Complete");
            }

            @Override
            public void onNext(@NonNull Long t) {
                System.out.println("next " + t);
            }
        };

        b.subscribe(c);

        Thread.sleep(1000);

        c.dispose();

        Thread.sleep(1000);

        if (true) return;

        ConnectableObservable<Triple<Long, ConnectableObservable<Long>, ConnectableObservable<Long>>> map = Observable.intervalRange(0, 20, 0, 200, TimeUnit.MILLISECONDS)
                .groupBy(it -> it % 2)
                .map(it -> {
                    ConnectableObservable<Long> current = it.replay(1);
                    ConnectableObservable<Long> replay = current.replay(5);
                    current.connect();
                    replay.connect();
                    return new Triple<>(it.getKey(), current, replay);
                }).replay();
        map.connect();

        System.out.println("take current " + map.flatMap(Triple::component3).firstElement().blockingGet());

        Disposable subscribe1 = map.filter(pair -> pair.component1() == 0)
                .flatMap(Triple::component3)
                .subscribe(a -> System.out.println("test1 " + a));

        DisposableObserver subscribe2 = new DisposableObserver<Long>() {
            @Override
            public void onNext(@NonNull Long aLong) {
                System.out.println("test2 " + aLong);
            }

            @Override
            public void onError(@NonNull Throwable e) {
                System.out.println("test2 error");
            }

            @Override
            public void onComplete() {
                System.out.println("test2 complete");
            }
        };

        map.filter(pair -> pair.component1() == 1)
                .flatMap(Triple::component3)
                .subscribe(subscribe2);

        Thread.sleep(1000);
        subscribe1.dispose();

        System.out.println("take current " + map.flatMap(Triple::component3).firstElement().blockingGet());

        Disposable subscribe3 = map.filter(pair -> pair.component1() == 0)
                .flatMap(Triple::component3)
                .subscribe(a -> System.out.println("test3 " + a));

        Thread.sleep(1000);
        subscribe2.dispose();

        System.out.println("take current " + map.flatMap(Triple::component3).firstElement().blockingGet());

        Thread.sleep(1000);
        subscribe3.dispose();

        System.out.println("take current " + map.flatMap(Triple::component3).firstElement().blockingGet());

        map.filter(pair -> pair.component1() == 1)
                .flatMap(Triple::component3)
                .subscribe(a -> System.out.println("test4 " + a));

        map.blockingLast();
    }

}
