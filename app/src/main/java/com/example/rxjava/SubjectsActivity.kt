package com.example.rxjava

import android.content.ContentValues.TAG
import android.os.Bundle
import android.util.Log
import android.view.View
import android.widget.Button
import android.widget.TextView
import androidx.appcompat.app.AppCompatActivity
import io.reactivex.rxjava3.core.Observer
import io.reactivex.rxjava3.disposables.Disposable
import io.reactivex.rxjava3.subjects.AsyncSubject
import io.reactivex.rxjava3.subjects.BehaviorSubject
import io.reactivex.rxjava3.subjects.PublishSubject
import io.reactivex.rxjava3.subjects.ReplaySubject


class SubjectsActivity : AppCompatActivity() {

    val subjectTv: TextView by lazy { findViewById(R.id.tvSubject) }
    val subjectPublishBtn: Button by lazy { findViewById(R.id.btnPublishSubject) }
    val subjectReplayBtn: Button by lazy { findViewById(R.id.btnReplaySubject) }
    val subjectBehaviorBtn: Button by lazy { findViewById(R.id.btnBehaviorSubject) }
    val subjectAsyncBtn: Button by lazy { findViewById(R.id.btnAsyncSubject) }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_subjects)

        subjectPublishBtn.setOnClickListener(object : View.OnClickListener {
            override fun onClick(view: View?) {
                subjectTv.text = ""
                doWorkPublishSubject()
            }
        })

        subjectReplayBtn.setOnClickListener(object : View.OnClickListener {
            override fun onClick(view: View?) {
                subjectTv.text = ""
                doWorkReplaySubject()
            }
        })

        subjectBehaviorBtn.setOnClickListener(object : View.OnClickListener {
            override fun onClick(view: View?) {
                subjectTv.text = ""
                doWorkBehaviorSubject()
            }
        })
        subjectAsyncBtn.setOnClickListener(object : View.OnClickListener {
            override fun onClick(view: View?) {
                subjectTv.text = ""
                doWorkAsyncSubject()
            }
        })


    }

    private fun doWorkAsyncSubject() {
        val source = AsyncSubject.create<Int>()

        source.subscribe(getFirstObserver()) // it will emit only 4 and onComplete
        source.onNext(1)
        source.onNext(2)
        source.onNext(3)
        source.subscribe(getSecondObserver())
        source.onNext(3)
        source.onNext(4)
        source.onComplete()
    }

    private fun doWorkBehaviorSubject() {
        val source = BehaviorSubject.create<Int>()
        source.subscribe(getFirstObserver()) // it will get 1, 2, 3, 4 and onComplete
        source.onNext(1)
        source.onNext(2)
        source.onNext(3)
        source.subscribe(getSecondObserver())
        source.onNext(4)
        source.onComplete()
    }

    private fun doWorkReplaySubject() {
        val source = ReplaySubject.create<Int>()

        source.subscribe(getFirstObserver()) // it will get 1, 2, 3, 4

        source.onNext(1)
        source.onNext(2)
        source.onNext(3)
        source.onNext(4)
        source.onNext(5)
        source.onComplete()
        source.subscribe(getSecondObserver())

    }

    private fun doWorkPublishSubject() {
        val source = PublishSubject.create<Int>()
        source.subscribe(getFirstObserver()) // it will get 1, 2, 3, 4 and onComplete
        source.onNext(1)
        source.onNext(2)
        source.onNext(3)
        source.subscribe(getSecondObserver())
        source.onComplete()

    }

    private fun getFirstObserver(): Observer<Int> {
        return object : Observer<Int> {
            override fun onSubscribe(d: Disposable) {
                Log.d(TAG, " First onSubscribe : " + d.isDisposed());
            }

            override fun onNext(t: Int) {
                subjectTv.append(" First onNext  " + t + "\n");

                Log.d(TAG, " First onNext " + t);
            }

            override fun onError(e: Throwable) {
                subjectTv.append(" First onError  " + e.message + "\n");
                Log.d(TAG, " First onError " + e.message + "\n");
            }

            override fun onComplete() {
                subjectTv.append(" First onComplete" + "\n");
                Log.d(TAG, " First onComplete" + "\n");
            }

        }
    }

    private fun getSecondObserver(): Observer<Int> {
        return object : Observer<Int> {
            override fun onSubscribe(d: Disposable) {
                Log.d(TAG, " Second  onSubscribe : " + d.isDisposed());
            }

            override fun onNext(t: Int) {
                subjectTv.append(" Second  onNext " + t + "\n");
                Log.d(TAG, " Second  onNext " + t + "\n");
            }

            override fun onError(e: Throwable) {
                subjectTv.append(" Second  " + e.message + "\n");
                Log.d(TAG, " Second  " + e.message + "\n");
            }

            override fun onComplete() {
                subjectTv.append(" Second  onComplete" + "\n");
                Log.d(TAG, " Second  onComplete" + "\n");
            }

        }
    }
}








