package com.example.rxjava

import android.os.Bundle
import android.util.Log
import androidx.appcompat.app.AppCompatActivity
import io.reactivex.rxjava3.android.schedulers.AndroidSchedulers
import io.reactivex.rxjava3.core.Flowable
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.Observer
import io.reactivex.rxjava3.core.Single
import io.reactivex.rxjava3.disposables.Disposable
import io.reactivex.rxjava3.schedulers.Schedulers

class LessonActivity : AppCompatActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_lesson_activity)


        val someObservable = Observable.just(1, 1, 1, 4, 5, 6, 7, 8, 9, 10)

        someObservable
            .filter {
                it <= 5
            }
            .repeat(2)
            .distinctUntilChanged()
            .map {
                it.toDouble()
            }.subscribe(
                {
                    Log.d("RxJava3", "OnNext $it")
                },
                {},
                {}
            )


        val subscriber1 = object : Observer<Int> {


            override fun onSubscribe(d: Disposable) {

                Log.d("RxJava3", "onSubscribe ")
            }

            override fun onNext(t: Int) {
                Log.d("RxJava3", "onNext ${t * 10}")
            }

            override fun onError(e: Throwable) {
                Log.d("RxJava3", "onError $e")
            }


            override fun onComplete() {
                Log.d("RxJava3", "onComplete ")
            }
        }
        val subscriber2 = object : Observer<Int> {

            override fun onSubscribe(d: Disposable) {
                Log.d("RxJava3", "onSubscribe 2 ")
            }

            override fun onNext(t: Int) {
                val divideByZero = t / 0
                Log.d("RxJava3", "onNext 2 $divideByZero")
            }

            override fun onError(e: Throwable) {
                Log.d("RxJava3", "onError 2$e")
            }

            override fun onComplete() {
                Log.d("RxJava3", "onComplete 2 ")
            }
        }

        someObservable
            .subscribeOn(Schedulers.newThread())
            .doOnNext {
                Log.d("RxJava3", "doOnNext ${Thread.currentThread().name}")
            }
//            .subscribeOn(Schedulers.io())
            .observeOn(AndroidSchedulers.mainThread())
            .doOnNext {
                Log.d("RxJava3", "doOnNext ${Thread.currentThread().name}")
            }
            .subscribe(
                {
                    Log.d("RxJava3", "onNext $it")
                },
                {},
                {}
            )


        Flowable
            .just(1, 1, 1, 4, 5, 6, 7, 8, 9, 10)
            .onBackpressureBuffer(4)
            .subscribe({
                Log.d(
                    "RxJava3", "onNext $it"
                )
            }, {}, {}
            )


        Single.just(1)
            .subscribe({

            }, {

            })

    }
}