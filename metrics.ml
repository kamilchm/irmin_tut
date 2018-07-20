open Lwt.Infix

type metric = {
  name : string;
  gauge: int64;
}

let metric_t =
  let open Irmin.Type in
  record "metric" (fun name gauge -> { name; gauge})
  |+ field "name" string (fun t -> t.name)
  |+ field "gauge" int64 (fun t -> t.gauge)
  |> sealr

let print m = Fmt.pr "%a\n%!" (Irmin.Type.pp_json metric_t) m

let merge =
  let open Irmin.Merge in
  like metric_t (pair string counter)
    (fun x -> x.name, x.gauge)
    (fun (name, gauge) -> {name; gauge})
  |> option

module Metric: Irmin.Contents.S with type t = metric = struct
  type t = metric
  let t = metric_t
  let merge = merge
  let pp = Irmin.Type.pp_json metric_t
  let of_string s =
    Irmin.Type.decode_json metric_t (Jsonm.decoder (`String s))
end

module Store = Irmin_unix.Git.FS.KV(Metric)
let config = Irmin_git.config "/tmp/irmin"
let info fmt = Irmin_unix.info ~author:"Kamil" fmt

let incr t =
  let path = ["vm"; "writes"] in
  let update_tree tree = 
    (Store.Tree.find tree [] >|= function
      | None   -> { name = "writesin kb/s"; gauge = 0L }
      | Some x -> { x with gauge = Int64.add x.gauge 1L }
    )
    >>= fun m ->
      Store.Tree.add tree [] m
    >|= fun tree ->
      Some tree
  in
  Store.with_tree ~info:(info "New write event") t path (function
    | Some tree -> update_tree tree
    | None -> Lwt.return None
  )

let () =
  Lwt_main.run begin
    Store.Repo.v config
    >>= fun repo -> Store.master repo
    >>= fun master -> incr master
    >>= fun () -> Store.clone ~src:master ~dst:"tmp"
    >>= fun tmp -> incr master
    >>= fun () -> incr tmp
    >>= fun () -> Store.merge ~info:(info "Merge tmp into master") tmp ~into:master
    >>= function
      | Error (`Conflict e) -> failwith e
      | Ok () ->
          Store.get master ["vm"; "writes"]
          >|= fun m -> Fmt.pr "Gauge is %Ld\n%!" m.gauge
  end
