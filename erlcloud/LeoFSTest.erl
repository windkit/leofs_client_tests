#!/usr/bin/env escript
%% -*- mode: erlang,erlang-indent-level: 4,indent-tabs-mode: nil -*-
%% ex: ft=erlang ts=4 sw=4 et

-include_lib("deps/erlcloud/include/erlcloud_aws.hrl").

-define(HOST,       "localhost").
-define(PORT,       8080).

-define(ACCESS_KEY_ID       , "05236").
-define(SECRET_ACCESS_KEY   , "802562235").
-define(SIGN_VER            , v4).

-define(BUCKET      , "teste").
-define(TEMPDATA    , "../temp_data/").

-define(SMALL_TEST_F    , ?TEMPDATA++"testFile").
-define(LARGE_TEST_F    , ?TEMPDATA++"testFile.large").

-define(CHUNK_SIZE,     5242880).

main(_Args)->
    ok = code:add_paths(["ebin",
                         "deps/erlcloud/ebin",
                         "deps/jsx/ebin",
                         "deps/meck/ebin",
                         "deps/lhttpc/ebin",
                         "deps/leo_commons/ebin/"]),
    ssl:start(),
    erlcloud:start(),

    SignVer = ?SIGN_VER,
    init(SignVer),
    createBucket(?BUCKET),

    %% Put Object Test
    putObject(?BUCKET, "test.simple",    ?SMALL_TEST_F),
    putObject(?BUCKET, "test.large",     ?LARGE_TEST_F),

    %% Multipart Upload Test
    mpObject(?BUCKET, "test.simple.mp",  ?SMALL_TEST_F),
    mpObject(?BUCKET, "test.large.mp",   ?LARGE_TEST_F),

    %% Object Metadata Test
    headObject(?BUCKET, "test.simple",   ?SMALL_TEST_F),
    headObject(?BUCKET, "test.large",    ?LARGE_TEST_F),
%% MP File ETag != MD5
%%    headObject(?BUCKET, "test.simple.mp", ?SMALL_TEST_F),
%%    headObject(?BUCKET, "test.large.mp", ?LARGE_TEST_F),

    %% Get Object Test
    getObject(?BUCKET, "test.simple",    ?SMALL_TEST_F),
    getObject(?BUCKET, "test.simple.mp", ?SMALL_TEST_F),
    getObject(?BUCKET, "test.large",     ?LARGE_TEST_F),
    getObject(?BUCKET, "test.large.mp",  ?LARGE_TEST_F),

    %% Get Not Exist Object Test
    getNotExist(?BUCKET, "test.noexist"),

    %% Range Get Object Test
    rangeObject(?BUCKET, "test.simple",      ?SMALL_TEST_F, 1, 4), 
    rangeObject(?BUCKET, "test.simple.mp",   ?SMALL_TEST_F, 1, 4), 
    rangeObject(?BUCKET, "test.large",       ?LARGE_TEST_F, 1048576, 10485760), 
    rangeObject(?BUCKET, "test.large.mp",    ?LARGE_TEST_F, 1048576, 10485760), 

    %% Copy Object Test
    copyObject(?BUCKET, "test.simple", "test.simple.copy"),
    getObject(?BUCKET, "test.simple.copy", ?SMALL_TEST_F),

%%    %% List Object Test
%%    listObject(?BUCKET, "", -1),
%%
%%    %% Delete All Object Test
%%    deleteAllObjects(?BUCKET),
%%    listObject(?BUCKET, "", 0),
%%
%%    %% Multiple Page List Object Test
%%    putDummyObjects(?BUCKET, "list/", 35, ?SMALL_TEST_F),
%%    pageListBucket(?BUCKET, "list/", 35, 10),
%%
%%    %% Multiple Delete
%%    multiDelete(?BUCKET, "list/", 10),
%%
%%    %% GET-PUT ACL
%%    setBucketAcl(?BUCKET, "private"),
%%    setBucketAcl(?BUCKET, "public-read"),
%%    setBucketAcl(?BUCKET, "public-read-write"),
    ok.

init(_SignVer) ->
    Conf = erlcloud_s3:new(
             ?ACCESS_KEY_ID,
             ?SECRET_ACCESS_KEY,
             ?HOST,
             ?PORT),
    Conf2 = Conf#aws_config{s3_scheme = "http://"},
    put(s3, Conf2).

createBucket(BucketName) ->
    Conf = get(s3),
    io:format("===== Create Bucket [~s] Start =====~n", [BucketName]),
    erlcloud_s3:create_bucket(BucketName, Conf),
    io:format("===== Create Bucket End =====~n"),
    io:format("~n"),
    ok.

putObject(BucketName, Key, Path) ->
    Conf = get(s3),
    io:format("===== Put Object [~s/~s] Start =====~n", [BucketName, Key]),
    {ok, Bin} = file:read_file(Path),
    erlcloud_s3:put_object(BucketName, Key, Bin, [], Conf),
    io:format("===== Put Object End =====~n"),
    io:format("~n"),
    ok.

mpObject(BucketName, Key, Path) ->
    Conf = get(s3),
    io:format("===== Multipart Upload Object [~s/~s] Start =====~n", [BucketName, Key]),
    {ok, Bin} = file:read_file(Path),
    {ok, MP} = erlcloud_s3:start_multipart(BucketName, Key, [], [], Conf),
    UploadId = proplists:get_value(uploadId, MP),
    {ok, Etags} = upload_parts(BucketName, Key, UploadId, Bin, [], Conf),
    erlcloud_s3:complete_multipart(BucketName, Key, UploadId, Etags, [], Conf),
    io:format("===== Multipart Upload Object End =====~n"),
    io:format("~n"),
    ok.

headObject(BucketName, Key, Path) ->
    Conf = get(s3),
    io:format("===== Head Object [~s/~s] Start =====~n", [BucketName, Key]),
    Meta = erlcloud_s3:get_object_metadata(BucketName, Key, Conf), 
    ETag = string:substr(proplists:get_value(etag, Meta), 2, 32),
    CL = list_to_integer(proplists:get_value(content_length, Meta)),
    {ok, Bin} = file:read_file(Path),
    MD5 = leo_hex:binary_to_hex(crypto:hash(md5, Bin)),
    FileSize = byte_size(Bin),
    io:format("ETag: ~s, Size: ~p~n", [ETag, CL]),
    if ETag =:= MD5, CL =:= FileSize ->
           ok;
       true ->
           io:format("Metadata [~s/~s] NOT Match, Size: ~p, MD5: ~s~n", [BucketName, Key, FileSize, MD5]),
           throw(error)
    end,
    io:format("===== Head Object End =====~n"),
    io:format("~n"),
    ok.

getObject(BucketName, Key, Path) ->
    Conf = get(s3),
    io:format("===== Get Object [~s/~s] Start =====~n", [BucketName, Key]),
    Obj = erlcloud_s3:get_object(BucketName, Key, Conf),
    Content = proplists:get_value(content, Obj),
    {ok, Bin} = file:read_file(Path),
    if Content =:= Bin ->
           ok;
       true ->
           io:format("Content NOT Match!~n"),
           throw(error)
    end,
    io:format("===== Get Object End =====~n"),
    io:format("~n"),
    ok.

getNotExist(BucketName, Key) ->
    Conf = get(s3),
    io:format("===== Get Not Exist Object [~s/~s] Start =====~n", [BucketName, Key]),
    try
        erlcloud_s3:get_object(BucketName, Key, Conf),
        io:format("Should NOT Exist!~n"),
        throw(error)
    catch 
        error:{aws_error,{http_error, 404, _, _}} ->
            ok;
        error:{aws_error,{http_error, 403, _, _}} ->
            ok
    end,
    io:format("===== Get Not Exist Object End =====~n"),
    io:format("~n"),
    ok.

rangeObject(BucketName, Key, Path, Start, End) ->
    Conf = get(s3),
    io:format("===== Range Get Object [~s/~s] (~p-~p) Start =====\n", [BucketName, Key, Start ,End]),
    RangeStr = io_lib:format("bytes=~p-~p", [Start, End]),
    Obj = erlcloud_s3:get_object(BucketName, Key, [{range, RangeStr}], Conf),
    Content = proplists:get_value(content, Obj),
    {ok, Bin} = file:read_file(Path),
    Len = End - Start + 1,
    <<_:Start/binary, Part:Len/binary, _/binary>> = Bin,
    if Content =:= Part ->
           ok;
       true ->
           io:format("Content NOT Match!~n"),
           throw(error)
    end,
    io:format("===== Get Object End =====~n"),
    io:format("~n"),
    ok.

copyObject(BucketName, Src, Dst) ->
    Conf = get(s3),
    io:format("===== Copy Object [~s/~s] -> [~s/~s] Start =====~n", [BucketName, Src, BucketName, Dst]),
    

upload_parts(Bucket, Key, UploadId, LargeObj, Headers, Config) ->
    upload_parts(Bucket, Key, UploadId, 1, LargeObj, Headers, Config, []).

upload_parts(Bucket, Key, UploadId, PartNum, Bin, Headers, Config, Acc) when byte_size(Bin) >= ?CHUNK_SIZE ->
    <<Part:?CHUNK_SIZE/binary, Rest/binary>> = Bin,
    {ok, Ret} = erlcloud_s3:upload_part(Bucket, Key, UploadId, PartNum, Part, Headers, Config),
    Etag = proplists:get_value(etag, Ret),
    upload_parts(Bucket, Key, UploadId, PartNum + 1, Rest, Headers, Config, [{PartNum, Etag}|Acc]);
upload_parts(Bucket, Key, UploadId, PartNum, Bin, Headers, Config, Acc) ->
    {ok, Ret} = erlcloud_s3:upload_part(Bucket, Key, UploadId, PartNum, Bin, Headers, Config),
    Etag = proplists:get_value(etag, Ret),
    {ok, [{PartNum, Etag}|Acc]}.
